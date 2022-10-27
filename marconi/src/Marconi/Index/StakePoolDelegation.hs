{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Marconi.Index.StakePoolDelegation where

import Control.Arrow ((&&&))
import Control.Concurrent qualified as IO
import Control.Concurrent.MVar qualified as IO
import Control.Exception
import Control.Monad (void, when)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import Data.Coerce
import Data.Either (lefts)
import Data.Foldable (forM_)
import Data.Function (on, (&))
import Data.List (sortBy)
import Data.Map qualified as M
import Data.Maybe qualified as P
import Data.Ratio (denominator, numerator, (%))
import Data.Sequence qualified as Seq
import Data.Text qualified as TS
import Data.VMap qualified as VMap
import Data.Word
import Database.SQLite.Simple qualified as SQL
import Streaming.Prelude qualified as S
import System.Environment qualified as IO
import Text.Printf

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as CS
import Cardano.Streaming qualified as CS

import Cardano.Ledger.Coin qualified as L
import Cardano.Ledger.Compactible qualified as L
import Cardano.Ledger.Credential qualified as LC
import Cardano.Ledger.Era qualified as LE
import Cardano.Ledger.Keys qualified as LK
import Cardano.Ledger.PoolDistr qualified as Pd
import Cardano.Ledger.Shelley.API.Wallet qualified as Shelley
import Cardano.Ledger.Shelley.EpochBoundary qualified as EB
import Cardano.Ledger.Shelley.EpochBoundary qualified as Shelley
import Cardano.Ledger.Shelley.LedgerState qualified as SL
import Cardano.Ledger.Shelley.LedgerState qualified as Shelley
import Ledger.Tx.CardanoAPI (withIsCardanoEra)
import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Shelley.Ledger qualified as O

import Marconi.Streaming.ChainSync (chainEventSource, rollbackRingBuffer)

-- * Event

type Event = (CS.EpochNo, M.Map (LK.KeyHash 'LK.StakePool (LE.Crypto (O.ShelleyEra O.StandardCrypto))) L.Coin)

toEvents :: S.Stream (S.Of CS.LedgerState) IO r -> S.Stream (S.Of Event) IO r
toEvents source = source
  & S.mapMaybe toNoByron
  & firstEventOfEveryEpoch
  where
    -- | Skip Byron era as it doesn't have staking
    toNoByron :: C.LedgerState -> Maybe Event
    toNoByron ls = if
      | Just epochNo <- getEpochNo ls
      , Just m <- getStakeMap ls -> Just (epochNo, m)
      | otherwise -> Nothing

    -- | We get LedgerState at every block from the @ledgerStates@
    -- streamer but we only want the first one of every epoch
    firstEventOfEveryEpoch :: S.Stream (S.Of Event) IO r -> S.Stream (S.Of Event) IO r
    firstEventOfEveryEpoch source = source
      & S.slidingWindow 2 -- zip stream with tail
      & S.mapMaybe (\case
                  ((e0, _) Seq.:<| t@ (e1, b) Seq.:<| Seq.Empty)
                    | succ e0 == e1 -> Just t
                    | e0 == e1 -> Nothing
                    | otherwise -> error $ "This should never happen: consequent epochs wider apart than by one: " <> show (e0, e1)
                  _ -> error "This should never happen"
              )

getEpochNo :: C.LedgerState -> Maybe CS.EpochNo
getEpochNo ledgerState' = case ledgerState' of
  C.LedgerStateByron _st                   -> Nothing
  C.LedgerStateShelley st                  -> fromState st
  C.LedgerStateAllegra st                  -> fromState st
  C.LedgerStateMary st                     -> fromState st
  C.LedgerStateAlonzo st                   -> fromState st
  CS.LedgerState (O.LedgerStateBabbage st) -> fromState st -- TODO pattern missing from cardano-node: is it there on master? if not create PR.
  where
    fromState = Just . SL.nesEL . O.shelleyLedgerState

getStakeMap
  :: C.LedgerState
  -> Maybe (M.Map (LK.KeyHash 'LK.StakePool (LE.Crypto (O.ShelleyEra O.StandardCrypto))) L.Coin)
getStakeMap ledgerState' = case ledgerState' of
  C.LedgerStateByron _st                   -> Nothing
  C.LedgerStateShelley st                  -> fromState st
  C.LedgerStateAllegra st                  -> fromState st
  C.LedgerStateMary st                     -> fromState st
  C.LedgerStateAlonzo st                   -> fromState st
  CS.LedgerState (O.LedgerStateBabbage st) -> fromState st -- TODO pattern missing from cardano-node: is it there on master? if not create PR.
  where
    fromState
      :: forall proto era c
       . (c ~ LE.Crypto era)
      => O.LedgerState (O.ShelleyBlock proto era)
      -> Maybe (M.Map (LK.KeyHash 'LK.StakePool (LE.Crypto era)) L.Coin)
    fromState st = Just res
      where
        nes = O.shelleyLedgerState st :: SL.NewEpochState era

        stakeSnapshot = Shelley._pstakeSet . Shelley.esSnapshots . Shelley.nesEs $ nes :: Shelley.SnapShot c
        stakes = Shelley.unStake $ Shelley._stake stakeSnapshot :: VMap.VMap VMap.VB VMap.VP (LC.Credential 'LK.Staking c) (L.CompactForm L.Coin)

        delegations :: VMap.VMap VMap.VB VMap.VB (LC.Credential 'LK.Staking c) (LK.KeyHash 'LK.StakePool c)
        delegations = Shelley._delegations stakeSnapshot

        res :: M.Map (LK.KeyHash 'LK.StakePool c) L.Coin
        res = M.fromListWith (+) $ map (\(a, b) -> (b, a)) $ P.catMaybes $ VMap.elems $
          VMap.mapWithKey (\cred spkHash -> (\c -> (L.fromCompact c, spkHash)) <$> VMap.lookup cred stakes) delegations

deriving instance Num L.Coin

indexer :: FilePath -> FilePath -> FilePath -> IO ()
indexer conf socket db = CS.ledgerStates conf socket
  & toEvents
  & sqlite db
  & S.effects

-- * Store in Sqlite

sqlite
  :: FilePath
  -> S.Stream (S.Of Event) IO r
  -> S.Stream (S.Of Event) IO r
sqlite db source = do
  c <- lift $ SQL.open db
  lift $ do
    SQL.execute_ c
      "CREATE TABLE IF NOT EXISTS stakepool_delegation (poolId TEXT NOT NULL, lovelace INT NOT NULL, epochId INT NOT NULL)"

  let loop source' = lift (S.next source') >>= \case
        Left r -> pure r
        Right (event@ (epochNo, map'), source'') -> do
          let rows = undefined
          lift $ forM_ (M.toList map') $ \(keyHash, coin) ->
            SQL.execute c
              "INSERT INTO stakepool_delegation (poolId, lovelace, epochId) VALUES (?, ?, ?)" $ let
              in (toPoolId keyHash, coerce coin :: Integer, coerce epochNo :: Word64)
          S.yield event
          loop source''
  loop source

-- | Convert stakepool's key hash to prefixed bech32 as described in CIP-5
toPoolId :: LK.KeyHash 'LK.StakePool O.StandardCrypto -> TS.Text
toPoolId kh = C.serialiseToBech32 (CS.StakePoolKeyHash kh)

-- * Tmp

hot2 = let
  (conf, socket) = preview
  in do
  home <- IO.getEnv "HOME"
  indexer (home <> "/" <> conf) (home <> "/" <> socket) "spd.db"

preview, preprod, mainnet :: (FilePath, FilePath)
preview = ( "preview/config/config.json"
          , "preview/socket/node.socket" )
preprod = ( "preprod/config/config.json"
          , "preprod/socket/node.socket" )
mainnet = ( "cardano/config/config.json"
          , "cardano/socket/node.socket" )

hot :: IO ()
hot = do
  home <- IO.getEnv "HOME"
  let (conf, socket) = preview
      indexer' =
        CS.ledgerStates (home <> "/" <> conf) (home <> "/" <> socket)
          & toEvents
          & S.chain (\case
                      (epochNo, m) -> do
                        putStrLn $ show epochNo <> " (" <> show (M.size m) <> ")"
                        forM_ (sortBy (flip compare `on` snd) $ M.toList m) $ \(k, v) ->
                          putStrLn $ "   " <> TS.unpack (toPoolId k) <> ": " <> show v
                      _ -> pure ()
                      )
          & S.effects
  indexer'
