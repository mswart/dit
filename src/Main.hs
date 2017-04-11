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

processRegularFile :: LevelDB.DB -> FilePath -> IO String
processRegularFile db path = do
    contents <- ByteString.readFile path
    let fileHash = hash contents :: Digest SHA384
    LevelDB.put db def (convert fileHash) contents
    return $ show fileHash


processEntry :: LevelDB.DB -> FilePath -> FilePath -> IO ()
processEntry db dir entry = do
    let path = combine dir entry
    stat <- getSymbolicLinkStatus path
    let (_:path1) = path
    putStrLn $ path1 ++ "\t\t uid=" ++ (show (fileOwner stat)) ++ ", gid=" ++ (show (fileGroup stat)) ++ ", mode=" ++ (showOct (fileMode stat) "") ++ ", size=" ++ (show (fileSize stat)) ++ ", mtime=" ++ (show (modificationTime stat))
    if isDirectory stat
        then walkRecursive db path
        else return ()
    if isRegularFile stat
        then do
            fileHash <- processRegularFile db path
            putStrLn $ "\t\t\t\t hash=" ++ fileHash
        else return ()


walkRecursive :: LevelDB.DB -> FilePath -> IO ()
walkRecursive db path = do
    entries <- listDirectory path
    mapM_ (processEntry db path) entries


commit :: [String] -> IO ()
commit [dbdir, name, dir] = do
    db <- LevelDB.open dbdir def{createIfMissing=True}
    setCurrentDirectory dir
    walkRecursive db "."
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
