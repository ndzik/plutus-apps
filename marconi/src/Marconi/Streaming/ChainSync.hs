{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE MultiWayIf       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeOperators    #-}
module Marconi.Streaming.ChainSync where

import Control.Concurrent qualified as IO
import Control.Exception qualified as IO
import Control.Monad (void)
import Control.Monad.Primitive (PrimState)
import Control.Monad.Trans.Class (lift)
import Data.Vector qualified as V
import Data.Vector.Generic qualified as VG
import Data.Vector.Generic.Mutable qualified as VGM
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward), withChainSyncEventStream)

chainEventSource
  :: FilePath -> C.NetworkId -> C.ChainPoint
  -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
chainEventSource socket networkId chainPoint = do
  m <- lift IO.newEmptyMVar
  lift $ void $ IO.forkIO $ withChainSyncEventStream socket networkId chainPoint $ S.mapM_ (IO.putMVar m)
  S.repeatM $ IO.takeMVar m

-- | Ignores rollbacks, passes on only RollForward events as
-- tuple (e, ChainTip)
ignoreRollbacks
  :: S.Stream (S.Of (ChainSyncEvent a)) IO r
  -> S.Stream (S.Of a) IO r
ignoreRollbacks = S.mapMaybe $ \case
  RollForward e _ct -> Just e
  _                 -> Nothing

data RollbackException = RollbackLocationNotFound C.ChainPoint C.ChainTip
  deriving (Eq, Show)
instance IO.Exception RollbackException

-- | Ring buffer that handles rollbacks, throws if point of rollback
-- is not found.
rollbackRingBuffer
  :: forall a r
   . Int
  -> S.Stream (S.Of (ChainSyncEvent a)) IO r
  -> S.Stream (S.Of a) IO r
rollbackRingBuffer bufferSize chainSyncEvents = do
  vector :: VG.Mutable V.Vector (PrimState IO) (a, C.ChainTip) <- lift (VGM.new bufferSize)

  let
    -- Fill phase, don't yield anything. Consume ChainSyncEvents and
    -- roll back to an earlier location in the vector in case of
    -- rollback.
    fill :: Int -> Int -> S.Stream (S.Of (ChainSyncEvent a)) IO r -> S.Stream (S.Of a) IO r
    fill i j source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (chainSyncEvent, source') -> case chainSyncEvent of
        RollForward a ct -> do
          lift $ VGM.unsafeWrite vector i (a, ct)
          let i' = (i + 1) `rem` bufferSize
              j' = j + 1
          if j' == bufferSize
            then fillYield i' source'
            else fill i' j' source'
        RollBackward cp ct -> rewind (cp, ct) i j source'

    -- Fill & yield phase. Buffer is full in the beginning, but will
    -- need to be refilled when a rollback occurs.
    fillYield :: Int -> S.Stream (S.Of (ChainSyncEvent a)) IO r -> S.Stream (S.Of a) IO r
    fillYield i source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (chainSyncEvent, source') -> case chainSyncEvent of
        RollForward a ct -> do
          (a', _) <- lift $ VGM.exchange vector i (a, ct)
          S.yield a'
          fillYield (i `rem` bufferSize) source'
        RollBackward cp ct -> rewind (cp, ct) i bufferSize source'

    rewind :: (C.ChainPoint, C.ChainTip) -> Int -> Int -> S.Stream (S.Of (ChainSyncEvent a)) IO r -> S.Stream (S.Of a) IO r
    rewind (cp, ct) i j source' = case cp of
      C.ChainPointAtGenesis -> fill 0 0 source'
      _ -> do
        maybeIndex <- lift $ VG.findIndex ((ct ==) . snd) <$> VG.unsafeFreeze vector
        case calculateRewind maybeIndex bufferSize i j of
          Just (i', j') -> fill i' j' source'
          _             -> lift $ IO.throwIO $ RollbackLocationNotFound cp ct

  if | bufferSize > 0 -> fill 0 0 chainSyncEvents
     | otherwise      -> ignoreRollbacks chainSyncEvents

calculateRewind :: Maybe Int -> Int -> Int -> Int -> Maybe (Int, Int)
calculateRewind maybeIndex vectorSize i j = case maybeIndex of
  Just lastValidI -> let
    i' = lastValidI + 1
    j' = j - (if i > i' then i - i' else i + (j - i'))
    in Just (i' `rem` vectorSize, j')
  _ -> Nothing
