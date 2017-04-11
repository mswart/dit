{-# LANGUAGE OverloadedStrings #-}

import Data.ByteArray (convert)
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
import Numeric (showOct)
import System.Directory (listDirectory, setCurrentDirectory)
import System.Environment (getArgs)
import System.FilePath
import System.Posix.Files


type BlobList = HashSet.HashSet ByteString.ByteString

processRegularFile :: LevelDB.DB -> FilePath -> BlobList -> IO BlobList
processRegularFile db path blobs = do
    contents <- ByteString.readFile path
    let fileHash = convert (hash contents :: Digest SHA384)
    if HashSet.member fileHash blobs
        then do
            putStrLn $ "\t skip blob - already known: " ++ (show fileHash)
            return blobs
        else do
            putStrLn $ "\t adding new blob: " ++ (show fileHash)
            LevelDB.put db def (convert fileHash) contents
            return $ HashSet.insert (convert fileHash) blobs


processEntry :: LevelDB.DB -> FilePath -> BlobList -> FilePath -> IO (BlobList)
processEntry db dir blobs entry = do
    let path = combine dir entry
    stat <- getSymbolicLinkStatus path
    let (_:path1) = path
    putStrLn $ path1 ++ "\t\t uid=" ++ (show (fileOwner stat)) ++ ", gid=" ++ (show (fileGroup stat)) ++ ", mode=" ++ (showOct (fileMode stat) "") ++ ", size=" ++ (show (fileSize stat)) ++ ", mtime=" ++ (show (modificationTime stat))
    if isDirectory stat
        then walkRecursive db path blobs
        else if isRegularFile stat
            then processRegularFile db path blobs
            else return blobs



walkRecursive :: LevelDB.DB -> FilePath -> BlobList -> IO (BlobList)
walkRecursive db path blobs = do
    entries <- listDirectory path
    foldM (processEntry db path) blobs entries


commit :: [String] -> IO ()
commit [dbdir, name, dir] = do
    setCurrentDirectory dir
    db <- LevelDB.open dbdir def{createIfMissing=True}
    knownBlobs <- buildBlobList db
    putStrLn $ "Number distinced known blobs = " ++ (show $ HashSet.size $ knownBlobs)
    knownBlobs' <- walkRecursive  db "." knownBlobs
    putStrLn $ "Number distinced known blobs = " ++ (show $ HashSet.size $ knownBlobs')
    unsafeClose db


buildBlobList :: LevelDB.DB -> IO (BlobList)
buildBlobList db = withIter db def $ \ iter ->
        SM.foldr HashSet.insert (HashSet.empty :: BlobList) $ keySlice iter AllKeys Asc


listBlobs :: [String] -> IO ()
listBlobs [dbdir] = do
    db <- LevelDB.open dbdir def
    known' <- buildBlobList db
    putStrLn $ show $ HashSet.size $ known'
    unsafeClose db


dispatch :: [(String, [String] -> IO ())]
dispatch =  [ ("commit", commit),
              ("list-blobs", listBlobs)
            ]


main :: IO ()
main = do
    (command:args) <- getArgs
    let (Just action) = lookup command dispatch
    action args
