-- | DB operations for storing and dumping SscGlobalState.

module Pos.DB.Ssc.GState
       ( getSscGlobalState
       , sscGlobalStateToBatch
       , initSscDB
       ) where

import           Universum

import           Data.Default (def)
import qualified Database.RocksDB as Rocks
import           Formatting (bprint, build, (%))
import qualified Formatting.Buildable

import           Pos.Core (HasCoreConfiguration, genesisVssCerts)
import           Pos.DB (MonadDB, MonadDBRead, RocksBatchOp (..))
import           Pos.DB.Error (DBError (DBMalformed))
import           Pos.DB.Functions (dbSerializeValue)
import           Pos.DB.GState.Common (gsGetBi, gsPutBi)
import           Pos.Ssc.Types (SscGlobalState (..))
import qualified Pos.Ssc.VssCertData as VCD
import           Pos.Util.Util (maybeThrow)

getSscGlobalState :: (MonadDBRead m) => m SscGlobalState
getSscGlobalState =
    maybeThrow (DBMalformed "SSC global state DB is not initialized") =<<
    gsGetBi sscKey

sscGlobalStateToBatch :: SscGlobalState -> SscOp
sscGlobalStateToBatch = PutGlobalState

initSscDB :: (MonadDB m) => m ()
initSscDB = gsPutBi sscKey (def {_sgsVssCertificates = vcd})
  where
    vcd = VCD.fromList . toList $ genesisVssCerts

----------------------------------------------------------------------------
-- Operation
----------------------------------------------------------------------------

data SscOp
    = PutGlobalState !SscGlobalState

instance Buildable SscOp where
    build (PutGlobalState gs) = bprint ("SscOp ("%build%")") gs

instance (HasCoreConfiguration) => RocksBatchOp SscOp where
    toBatchOp (PutGlobalState gs) = [Rocks.Put sscKey (dbSerializeValue gs)]

----------------------------------------------------------------------------
-- Key
----------------------------------------------------------------------------

sscKey :: ByteString
sscKey = "ssc/"
