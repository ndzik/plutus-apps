{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings  #-}

module StakePoolDelegation where

import Control.Concurrent qualified as IO
import Control.Monad (forever, void)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Function (on, (&))
import Data.Map qualified as Map
import Streaming.Prelude qualified as S
import System.Directory qualified as IO
import System.FilePath ((</>))

import Hedgehog (Property, assert, (===))
import Hedgehog.Extras.Test qualified as HE
import Hedgehog.Extras.Test.Base qualified as HE
import Test.Base qualified as H
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testPropertyNamed)

import Cardano.Api.Shelley qualified as C
import Cardano.Streaming (ChainSyncEventException (NoIntersectionFound), withChainSyncEventStream)
import Cardano.Streaming qualified as CS
import Helpers
import Marconi.Index.StakePoolDelegation qualified as M
import Testnet.Cardano qualified as TN

tests :: TestTree
tests = testGroup "StakePoolDelegation"
  [ testPropertyNamed "prop_stake_delegation" "test" test
  ]

test :: Property
test = H.integration . HE.runFinallies . workspace "chairman" $ \tempAbsBasePath' -> do

  -- start testnet
  base <- HE.noteM $ liftIO . IO.canonicalizePath =<< HE.getProjectBase
  let testnetOptions = TN.defaultTestnetOptions
        { TN.epochLength = 10 -- 0.2 * 10 = 2 seconds
        }
  (localNodeConnectInfo, conf, runtime) <-
    startTestnet_ testnetOptions base tempAbsBasePath'
  socketPath <- getSocketPathAbs conf runtime

  -- start indexer
  chan <- liftIO IO.newChan
  void $ liftIO $ IO.forkIO $ void $ S.effects $ let
       source = CS.ledgerStates (TN.configurationFile runtime) socketPath
    in source & S.chain (\e -> p2 "got event" $ M.getEpochNo e)
              & M.toEvents
              & M.sqlite ":memory:"

  -- start chainSyncEventSource
  -- chan <- liftIO IO.newChan
  -- void $ liftIO $ IO.forkIO $ void $ S.effects $ let
  --      source = CS.chainSyncEventSource socketPath (getNetworkId runtime) C.ChainPointAtGenesis
  --   in source & S.chain (\e -> p "got event 0")

  liftIO $ forever $ do
    IO.readChan chan
    putStrLn "got event"

  {- Process:
    - register  a stakepool: done in testnet
    - generate a staking address for a user
    - stake the user's lovelace to that pool, let's call this event "stake1" (referred to below)
    - wait for the turn of epoch
    - stake to another pool (or unstake?), let's call this event "stake2"
    - wait for the turn of next epoch: observe event "stake1" realize: stake appears in indexer
    - wait for the turn of next epoch: observe "stake2" realize: stake appears in another pool (or disappears from pool1 if it was just unstaked)
  -}

  True === True
