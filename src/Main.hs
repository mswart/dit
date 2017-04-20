{-# LANGUAGE OverloadedStrings #-}

import Data.Bits
import Data.ByteArray (convert)
import Data.UUID.Types (UUID)
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (POSIXTime, posixSecondsToUTCTime)
import Data.Word (Word8)
import Crypto.Hash (hash, Digest, SHA384)
import Control.Monad.State as State
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Char8 as C
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
import System.Directory (listDirectory, setCurrentDirectory, createDirectory, createDirectoryIfMissing, setModificationTime)
import System.Environment (getArgs)
import System.FilePath
import System.Posix.Files
import System.Posix.Types


type BlobList = HashSet.HashSet ByteString.ByteString
type CommitState = ([LevelDB.BatchOp], [FileRow], BlobList)
type FileRow = (
        UUID, {- system_id -}
        FilePath,  {- path -}
        Int, {- mode -}
        Int, {- uid -}
        Int, {- gid -}
        UTCTime, {- ctime -}
        UTCTime, {- mtime -}
        Maybe Int, {- rdev -}
        Maybe Int, {- size -}
        Maybe (PG.Binary ByteString.ByteString) {- blob -}
    )


convertTime :: EpochTime -> UTCTime
convertTime t =
    posixSecondsToUTCTime (realToFrac t :: POSIXTime)


buildFileRow :: UUID -> FilePath -> FileStatus -> Maybe ByteString.ByteString -> FileRow
buildFileRow system_id path stat fileHash =
    (system_id, path1, mode, uid, gid, mtime, atime, rdev, size, blob)
    where (_:path1) = path
          mode = fromEnum $ fileMode stat
          uid = fromEnum $ fileOwner stat
          gid = fromEnum $ fileGroup stat
          mtime = convertTime $ modificationTime stat
          atime = convertTime $ statusChangeTime stat
          stat_rdev = fromEnum $ specialDeviceID stat
          rdev = if stat_rdev == 0 then Nothing else Just stat_rdev
          size = Just $ fromEnum $ fileSize stat
          blob = fmap PG.Binary fileHash


processTypedEntry :: UUID -> FilePath -> FileStatus -> CommitState -> IO CommitState
processTypedEntry system_id path stat (batch, inserts, blobs)
    | isRegularFile stat = do
        contents <- ByteString.readFile path
        return $ addIfNew system_id path stat contents batch inserts blobs
    | isDirectory stat = do
        let newRow = buildFileRow system_id path stat Nothing
            new_state = (batch, newRow:inserts, blobs)
        walkRecursive system_id path new_state
    | isSymbolicLink stat = do
        linkContents <- readSymbolicLink path
        let contents = C.pack linkContents
        return $ addIfNew system_id path stat contents batch inserts blobs
    | isBlockDevice stat || isCharacterDevice stat = do
        let newRow = buildFileRow system_id path stat Nothing
        return (batch, newRow:inserts, blobs)
    {- named pipe and socket are runtime objects, no need to record them: -}
    | isNamedPipe stat = return (batch, inserts, blobs)
    | isSocket stat = return (batch, inserts, blobs)
    where addIfNew system_id path stat contents batch inserts blobs =
            if HashSet.member fileHash blobs
            then (batch, newRow:inserts, blobs)
            else do
                (LevelDB.Put fileHash contents:batch,
                 newRow:inserts,
                 HashSet.insert fileHash blobs)
            where fileHash = convert (hash contents :: Digest SHA384)
                  newRow = buildFileRow system_id path stat (Just fileHash)


processEntry :: UUID -> FilePath -> CommitState -> FilePath -> IO (CommitState)
processEntry system_id dir state entry = do
    let path = combine dir entry
    stat <- getSymbolicLinkStatus path
    processTypedEntry system_id path stat state


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
    putStrLn $ "\tcurrently distinced known blobs: "
         ++ (show $ HashSet.size $ knownBlobs)

    [PG.Only id] <- PG.returning pgc "INSERT INTO systems (name)\
                                      \ VALUES (?) RETURNING id" [PG.Only name]
    putStrLn $ "Defined system " ++ name ++ " - " ++ (show id)

    root_stat <- getSymbolicLinkStatus "."
    let root_row = buildFileRow id "./" root_stat Nothing

    (batch, inserts, knownBlobs')
         <- walkRecursive id "." ([], [root_row], knownBlobs)

    putStrLn $ "Commit new blobs to LevelDB"
    LevelDB.write ldb def batch

    putStrLn $ "Insert files into PostgreSQL"
    PG.executeMany pgc "INSERT INTO files \
        \ (system_id, path, mode, uid, gid, ctime, mtime, rdev, size, blob) \
        \ VALUES (?,?,?,?,?,?,?,?,?,?)" inserts

    putStrLn $ "Number distinced known blobs = "
         ++ (show $ HashSet.size $ knownBlobs')

    unsafeClose ldb
    PG.close pgc


checkoutFile :: FilePath -> LevelDB.DB -> (String, Int, Int, Int, UTCTime, Maybe Int, Maybe ByteString.ByteString) -> IO ()
checkoutFile base ldb (_:path, mode, uid, gid, mtime, _, Just blob)
    | fileType == regularFileMode = do
        Just contents <- LevelDB.get ldb def blob
        ByteString.writeFile realpath contents
        setOwnerAndGroup realpath (toEnum uid) (toEnum gid)
        setFileMode realpath $ toEnum mode
        setModificationTime realpath mtime
    | fileType == symbolicLinkMode = do
        Just contents <- LevelDB.get ldb def blob
        createSymbolicLink (C.unpack contents) realpath
        setSymbolicLinkOwnerAndGroup realpath (toEnum uid) (toEnum gid)
    where realpath = (combine base path)
          fmode = toEnum mode
          fileType = (.&.) fmode fileTypeModes
checkoutFile base ldb (_:path, mode, uid, gid, mtime, Nothing, Nothing) = do
    createDirectory realpath
    setOwnerAndGroup realpath (toEnum uid) (toEnum gid)
    setFileMode realpath $ toEnum mode
    setModificationTime realpath mtime
    where realpath = (combine base path)
checkoutFile base ldb (_:path, mode, uid, gid, mtime, Just rdev, Nothing) = do
    createDevice realpath (toEnum mode) (toEnum rdev)
    setOwnerAndGroup realpath (toEnum uid) (toEnum gid)
    setFileMode realpath $ toEnum mode
    setModificationTime realpath mtime
    where realpath = (combine base path)


checkout :: [String] -> IO ()
checkout [dbdir, name_or_uuid, dir] = do
    ldb <- LevelDB.open dbdir def{createIfMissing=True, cacheSize=256*1024*1024}
    pgc <- PG.connectPostgreSQL "dbname=dit"

    ((id, name):_) <- PG.query pgc "SELECT id, name FROM systems \
                   \ WHERE ? in (id::text, name)" [name_or_uuid]
    putStrLn $ "Checking out system " ++ name ++ " - " ++ (show (id :: UUID))

    createDirectoryIfMissing False dir

    PG.forEach pgc "SELECT path, mode, uid, gid, mtime, rdev, blob \
        \FROM files WHERE system_id = ? AND path != '/' ORDER BY path ASC" [id] $ checkoutFile dir ldb

    unsafeClose ldb
    PG.close pgc


buildBlobList :: LevelDB.DB -> IO (BlobList)
buildBlobList db = withIter db def $ \ iter ->
        SM.foldl' (flip HashSet.insert) (HashSet.empty :: BlobList)
             $ keySlice iter AllKeys Asc


listBlobs :: [String] -> IO ()
listBlobs [dbdir] = do
    db <- LevelDB.open dbdir def
    known' <- buildBlobList db
    putStrLn $ show $ HashSet.size $ known'
    unsafeClose db


printSystems :: (UUID, String, PGTime.ZonedTimestamp, Int) -> IO ()
printSystems (id, name, commited_at, file_count) = do
    putStrLn $ (show id) ++ "  |  \"" ++ name
         ++ "\"  |  " ++ (show commited_at)
         ++ "  |  " ++ (show file_count) ++ " files"

listSystems :: [String] -> IO ()
listSystems [] = do
    pgc <- PG.connectPostgreSQL "dbname=dit"
    PG.forEach_ pgc "SELECT id, name, commited_at, \
        \(SELECT count(*) FROM files WHERE files.system_id = systems.id) \
        \FROM systems ORDER BY commited_AT ASC" printSystems


pactDb :: [String] -> IO ()
pactDb [dbdir] = do
    ldb <- LevelDB.open dbdir def {cacheSize=256*1024*1024}
    printDbSize ldb
    putStrLn "Trigger compaction"
    compactRange ldb range
    printDbSize ldb
  where
    printDbSize ldb = do
        s <- approximateSize ldb range
        putStrLn $ "Approximate DB size: " ++ (show s)
    range = (ByteString.singleton (toEnum 0 :: Word8), ByteString.singleton ((toEnum  255) :: Word8))



dispatch :: [(String, [String] -> IO ())]
dispatch =  [ ("commit", commit),
              ("checkout", checkout),
              ("list-blobs", listBlobs),
              ("list-systems", listSystems),
              ("pack-db", pactDb)
            ]


main :: IO ()
main = do
    (command:args) <- getArgs
    let (Just action) = lookup command dispatch
    action args
