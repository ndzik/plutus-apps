{-# LANGUAGE LambdaCase     #-}
{-# LANGUAGE NamedFieldPuns #-}

module Helpers where

import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Hedgehog (MonadTest, assert)
import System.FilePath ((</>))

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Data.Set qualified as Set
import GHC.Stack qualified as GHC
import Hedgehog qualified as H
import Hedgehog.Extras.Stock.IO.Network.Sprocket qualified as IO
import Hedgehog.Extras.Test qualified as HE
import Hedgehog.Extras.Test.Base qualified as H
import Ouroboros.Network.Protocol.LocalTxSubmission.Type (SubmitResult (SubmitFail, SubmitSuccess))
import System.Directory qualified as IO
import System.Environment qualified as IO
import System.IO.Temp qualified as IO
import System.Info qualified as IO
import Testnet.Cardano qualified as TN
import Testnet.Conf qualified as TC (Conf (..), ProjectBase (ProjectBase), YamlFilePath (YamlFilePath), mkConf)

-- * Start testnet

startTestnet :: FilePath -> FilePath -> H.Integration (String, C.NetworkId, FilePath)
startTestnet base tempAbsBasePath' = do
  (_, conf, tn) <- startTestnet_ TN.defaultTestnetOptions base tempAbsBasePath'
  let tempAbsPath = tempAbsBasePath' <> "/"
  socketPathAbs <- getSocketPathAbs conf tn
  pure (socketPathAbs, getNetworkId tn, tempAbsPath)

startTestnet_
  :: TN.TestnetOptions
  -> FilePath
  -> FilePath
  -> H.Integration (C.LocalNodeConnectInfo C.CardanoMode, TC.Conf, TN.TestnetRuntime)
startTestnet_ testnetOptions base tempAbsBasePath' = do
  configurationTemplate <- H.noteShow $ base </> "configuration/defaults/byron-mainnet/configuration.yaml"
  conf@TC.Conf { TC.tempBaseAbsPath, TC.tempAbsPath } <- HE.noteShowM $
    TC.mkConf (TC.ProjectBase base) (TC.YamlFilePath configurationTemplate)
      (tempAbsBasePath' <> "/")
      Nothing
  tn <- TN.testnet testnetOptions conf

  -- Boilerplate codecs used for protocol serialisation.  The number
  -- of epochSlots is specific to each blockchain instance. This value
  -- what the cardano main and testnet uses. Only applies to the Byron
  -- era.
  socketPathAbs <- getSocketPathAbs conf tn
  let epochSlots = C.EpochSlots 21600
      localNodeConnectInfo =
        C.LocalNodeConnectInfo
          { C.localConsensusModeParams = C.CardanoModeParams epochSlots
          , C.localNodeNetworkId = getNetworkId tn
          , C.localNodeSocketPath = socketPathAbs
          }

  pure (localNodeConnectInfo, conf, tn)

getNetworkId :: TN.TestnetRuntime -> C.NetworkId
getNetworkId tn = C.Testnet $ C.NetworkMagic $ fromIntegral (TN.testnetMagic tn)

getSocketPathAbs :: (MonadTest m, MonadIO m) => TC.Conf -> TN.TestnetRuntime -> m FilePath
getSocketPathAbs conf tn = do
  let tempAbsPath = TC.tempAbsPath conf
  socketPath <- IO.sprocketArgumentName <$> headM (TN.nodeSprocket <$> TN.bftNodes tn)
  socketPathAbs <- H.note =<< (liftIO $ IO.canonicalizePath $ tempAbsPath </> socketPath)
  pure socketPathAbs

--

readAs :: (C.HasTextEnvelope a, MonadIO m, MonadTest m) => C.AsType a -> FilePath -> m a
readAs as path = H.leftFailM . liftIO $ C.readFileTextEnvelope as path

-- * Talk to node

emptyTxBodyContent :: C.Lovelace -> C.ProtocolParameters -> C.TxBodyContent C.BuildTx C.AlonzoEra
emptyTxBodyContent fee pparams = C.TxBodyContent
  { C.txIns              = []
  , C.txInsCollateral    = C.TxInsCollateralNone
  , C.txInsReference     = C.TxInsReferenceNone
  , C.txOuts             = []
  , C.txTotalCollateral  = C.TxTotalCollateralNone
  , C.txReturnCollateral = C.TxReturnCollateralNone
  , C.txFee              = C.TxFeeExplicit C.TxFeesExplicitInAlonzoEra fee
  , C.txValidityRange    = (C.TxValidityNoLowerBound, C.TxValidityNoUpperBound C.ValidityNoUpperBoundInAlonzoEra)
  , C.txMetadata         = C.TxMetadataNone
  , C.txAuxScripts       = C.TxAuxScriptsNone
  , C.txExtraKeyWits     = C.TxExtraKeyWitnessesNone
  , C.txProtocolParams   = C.BuildTxWith $ Just pparams
  , C.txWithdrawals      = C.TxWithdrawalsNone
  , C.txCertificates     = C.TxCertificatesNone
  , C.txUpdateProposal   = C.TxUpdateProposalNone
  , C.txMintValue        = C.TxMintNone
  , C.txScriptValidity   = C.TxScriptValidityNone
  }

getAlonzoProtocolParams :: (MonadIO m, MonadTest m) => C.LocalNodeConnectInfo C.CardanoMode -> m C.ProtocolParameters
getAlonzoProtocolParams localNodeConnectInfo = H.leftFailM . H.leftFailM . liftIO
  $ C.queryNodeLocalState localNodeConnectInfo Nothing
  $ C.QueryInEra C.AlonzoEraInCardanoMode
  $ C.QueryInShelleyBasedEra C.ShelleyBasedEraAlonzo C.QueryProtocolParameters

findUTxOByAddress
  :: (MonadIO m, MonadTest m)
  => C.LocalNodeConnectInfo C.CardanoMode -> C.AddressAny -> m (C.UTxO C.AlonzoEra)
findUTxOByAddress localNodeConnectInfo address = let
  query = C.QueryInShelleyBasedEra C.ShelleyBasedEraAlonzo $ C.QueryUTxO $
    C.QueryUTxOByAddress $ Set.singleton address
  in
  H.leftFailM . H.leftFailM . liftIO $ C.queryNodeLocalState localNodeConnectInfo Nothing $
    C.QueryInEra C.AlonzoEraInCardanoMode query

submitTx :: (MonadIO m, MonadTest m) => C.LocalNodeConnectInfo C.CardanoMode -> C.Tx C.AlonzoEra -> m ()
submitTx localNodeConnectInfo tx = do
  submitResult :: SubmitResult (C.TxValidationErrorInMode C.CardanoMode) <-
    liftIO $ C.submitTxToNodeLocal localNodeConnectInfo $ C.TxInMode tx C.AlonzoEraInCardanoMode
  failOnTxSubmitFail submitResult
  where
    failOnTxSubmitFail :: (Show a, MonadTest m) => SubmitResult a -> m ()
    failOnTxSubmitFail = \case
      SubmitFail reason -> H.failMessage GHC.callStack $ "Transaction failed: " <> show reason
      SubmitSuccess     -> pure ()

-- TODO: remove when this is exported from hedgehog-extras/src/Hedgehog/Extras/Test/Base.hs
headM :: (MonadTest m, GHC.HasCallStack) => [a] -> m a
headM (a:_) = return a
headM []    = GHC.withFrozenCallStack $ H.failMessage GHC.callStack "Cannot take head of empty list"

workspace :: (MonadTest m, MonadIO m, GHC.HasCallStack) => FilePath -> (FilePath -> m ()) -> m ()
workspace prefixPath f = GHC.withFrozenCallStack $ do
  systemTemp <- case IO.os of
    "darwin" -> pure "/tmp"
    _        -> H.evalIO IO.getCanonicalTemporaryDirectory
  maybeKeepWorkspace <- H.evalIO $ IO.lookupEnv "KEEP_WORKSPACE"
  let systemPrefixPath = systemTemp <> "/" <> prefixPath
  H.evalIO $ IO.createDirectoryIfMissing True systemPrefixPath
  ws <- H.evalIO $ IO.createTempDirectory systemPrefixPath "test"
  H.annotate $ "Workspace: " <> ws
  -- liftIO $ IO.writeFile (ws <> "/module") callerModuleName
  f ws
  when (IO.os /= "mingw32" && maybeKeepWorkspace /= Just "1") $ do
    H.evalIO $ IO.removeDirectoryRecursive ws
