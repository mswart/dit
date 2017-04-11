{-# LANGUAGE OverloadedStrings #-}

import Data.ByteArray (convert)
import Data.UUID.Types (UUID)
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (POSIXTime, posixSecondsToUTCTime)
import Crypto.Hash (hash, Digest, SHA384)
import Control.Monad.State as State
import qualified Data.ByteString as ByteString
import Data.Default (def)
import qualified Data.Foldable as F
import qualified Data.HashSet as HashSet
import qualified Data.Stream.Monadic as SM
import Database.LevelDB.Base as LevelDB
import Database.LevelDB.Internal (unsafeClose)
import Database.LevelDB.Iterator (withIter)
import Database.LevelDB.Streaming (keySlice, KeyRange(..), Direction(..))
import qualified Database.PostgreSQL.Simple as PG
import qualified Database.PostgreSQL.Simple.Time as PGTime
import Numeric (showOct)
import System.Directory (listDirectory, setCurrentDirectory)
import System.Environment (getArgs)
import System.FilePath
import System.Posix.Files
import System.Posix.Types


type BlobList = HashSet.HashSet ByteString.ByteString
type CommitState = ([LevelDB.BatchOp], [FileRow], BlobList)
type FileRow = (UUID, FilePath, Int, Int, Int, UTCTime, UTCTime, Maybe Int, Maybe Int, Maybe (PG.Binary ByteString.ByteString))


convertTime :: EpochTime -> UTCTime
convertTime t =
    posixSecondsToUTCTime (realToFrac t :: POSIXTime)


processRegularFile :: UUID -> FilePath -> FileStatus -> CommitState -> IO CommitState
processRegularFile system_id path stat (batch, inserts, blobs) = do
    contents <- ByteString.readFile path
    let fileHash = convert (hash contents :: Digest SHA384)
    let (_:path1) = path
    let newRow = (system_id, path1, fromEnum $ fileMode stat, fromEnum $ fileOwner stat, fromEnum $ fileGroup stat, convertTime $ modificationTime stat, convertTime $ statusChangeTime stat, Nothing, Just $ fromEnum $ fileSize stat, Just $ PG.Binary fileHash)
    if HashSet.member fileHash blobs
        then return (batch, inserts, blobs)
        else do
            return $ (LevelDB.Put (convert fileHash) contents:batch,
                      newRow:inserts,
                      HashSet.insert (convert fileHash) blobs)


processEntry :: UUID -> FilePath -> CommitState -> FilePath -> IO (CommitState)
processEntry system_id dir state entry = do
    let path = combine dir entry
    stat <- getSymbolicLinkStatus path
    let (_:path1) = path
    if isDirectory stat
        then walkRecursive system_id path state
        else if isRegularFile stat
            then processRegularFile system_id path stat state
            else return state


walkRecursive :: UUID -> FilePath -> CommitState -> IO (CommitState)
walkRecursive system_id path state = do
    entries <- listDirectory path
    foldM (processEntry system_id path) state entries


commit :: [String] -> IO ()
commit [dbdir, name, dir] = do
    setCurrentDirectory dir
    ldb <- LevelDB.open dbdir def{createIfMissing=True}
    pgc <- PG.connectPostgreSQL "dbname=dit"

    putStrLn $ "Loading cache of known blobs ..."
    knownBlobs <- buildBlobList ldb
    putStrLn $ "\tcurrently distinced known blobs: " ++ (show $ HashSet.size $ knownBlobs)

    [PG.Only id] <- PG.returning pgc "INSERT INTO systems (name) VALUES (?) RETURNING id" [PG.Only name]
    putStrLn $ "Defined system " ++ name ++ " - " ++ (show id)

    (batch, inserts, knownBlobs') <- walkRecursive id "." ([], [], knownBlobs)

    putStrLn $ "Commit new blobs to LevelDB"
    LevelDB.write ldb def batch

    putStrLn $ "Insert files into PostgreSQL"
    PG.executeMany pgc "INSERT INTO files (system_id, path, mode, uid, gid, ctime, mtime, rdev, size, blob) VALUES (?,?,?,?,?,?,?,?,?,?)" inserts

    putStrLn $ "Number distinced known blobs = " ++ (show $ HashSet.size $ knownBlobs')

    unsafeClose ldb
    PG.close pgc


buildBlobList :: LevelDB.DB -> IO (BlobList)
buildBlobList db = withIter db def $ \ iter ->
        SM.foldl' (flip HashSet.insert) (HashSet.empty :: BlobList) $ keySlice iter AllKeys Asc


listBlobs :: [String] -> IO ()
listBlobs [dbdir] = do
    db <- LevelDB.open dbdir def
    known' <- buildBlobList db
    putStrLn $ show $ HashSet.size $ known'
    unsafeClose db


printSystems :: (UUID, String, PGTime.ZonedTimestamp) -> IO ()
printSystems (id, name, commited_at) = do
    putStrLn $ (show id) ++ " \"" ++ name ++ "\" " ++ (show commited_at)

listSystems :: [String] -> IO ()
listSystems [] = do
    pgc <- PG.connectPostgreSQL "dbname=dit"
    PG.forEach_ pgc "SELECT id, name, commited_at FROM systems ORDER BY commited_AT ASC" printSystems


dispatch :: [(String, [String] -> IO ())]
dispatch =  [ ("commit", commit),
              ("list-blobs", listBlobs),
              ("list-systems", listSystems)
            ]


main :: IO ()
main = do
    (command:args) <- getArgs
    let (Just action) = lookup command dispatch
    action args
