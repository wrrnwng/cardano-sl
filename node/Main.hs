{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE TypeOperators       #-}

module Main
       ( main
       ) where

import           Universum

import           Data.Maybe (fromJust)
import           System.Wlog (LoggerName, logInfo)

import           Pos.Binary ()
import           Pos.Client.CLI (CommonNodeArgs (..), NodeArgs (..),
                     SimpleNodeArgs (..))
import qualified Pos.Client.CLI as CLI
import           Pos.Crypto (ProtocolMagic)
import           Pos.Infra.Ntp.Configuration (NtpConfiguration)
import           Pos.Launcher (HasConfigurations, NodeParams (..),
                     loggerBracket, runNodeReal, withConfigurations)
import           Pos.Launcher.Configuration (AssetLockPath (..))
import           Pos.Ssc.Types (SscParams)
import           Pos.Util (logException)
import           Pos.Util.CompileInfo (HasCompileInfo, withCompileInfo)
import           Pos.Util.UserSecret (usVss)
import           Pos.Worker.Update (updateTriggerWorker)

loggerName :: LoggerName
loggerName = "node"

actionWithoutWallet
    :: ( HasConfigurations
       , HasCompileInfo
       )
    => ProtocolMagic
    -> SscParams
    -> NodeParams
    -> IO ()
actionWithoutWallet pm sscParams nodeParams =
    runNodeReal pm nodeParams sscParams [updateTriggerWorker]

action
    :: ( HasConfigurations
       , HasCompileInfo
       )
    => SimpleNodeArgs
    -> NtpConfiguration
    -> ProtocolMagic
    -> IO ()
action (SimpleNodeArgs (cArgs@CommonNodeArgs {..}) (nArgs@NodeArgs {..})) ntpConfig pm = do
    CLI.printInfoOnStart cArgs ntpConfig
    logInfo "Wallet is disabled, because software is built w/o it"
    currentParams <- CLI.getNodeParams loggerName cArgs nArgs

    let vssSK = fromJust $ npUserSecret currentParams ^. usVss
    let sscParams = CLI.gtSscParams cArgs vssSK (npBehaviorConfig currentParams)

    actionWithoutWallet pm sscParams currentParams

main :: IO ()
main = withCompileInfo $ do
    args@(CLI.SimpleNodeArgs commonNodeArgs _) <- CLI.getSimpleNodeOptions
    let loggingParams = CLI.loggingParams loggerName commonNodeArgs
    let conf = CLI.configurationOptions (CLI.commonArgs commonNodeArgs)
    let blPath = AssetLockPath <$> cnaAssetLockPath commonNodeArgs
    loggerBracket loggingParams . logException "node" $
        withConfigurations blPath conf $ action args
