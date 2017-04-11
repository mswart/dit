{-# LANGUAGE OverloadedStrings #-}

import Crypto.Hash
import qualified Data.ByteString as B
import qualified Data.Foldable as F
import Numeric (showOct)
import System.Directory
import System.Environment
import System.FilePath
import System.Posix.Files

calcFileHash :: FilePath -> IO String
calcFileHash path = do
    contents <- B.readFile path
    return $ show (hash contents :: Digest SHA384)

listDirectory :: FilePath -> IO [FilePath]
listDirectory path =
    (filter f) <$> (getDirectoryContents path)
    where f filename = filename /= "." && filename /= ".."

processEntry :: FilePath -> FilePath -> IO ()
processEntry dir entry = do
    let path = combine dir entry
    stat <- getSymbolicLinkStatus path
    let (_:path1) = path
    putStrLn $ path1 ++ "\t\t uid=" ++ (show (fileOwner stat)) ++ ", gid=" ++ (show (fileGroup stat)) ++ ", mode=" ++ (showOct (fileMode stat) "") ++ ", size=" ++ (show (fileSize stat)) ++ ", mtime=" ++ (show (modificationTime stat))
    if isDirectory stat
        then walkRecursive path
        else return ()
    if isRegularFile stat
        then do
            fileHash <- calcFileHash path
            putStrLn $ "\t\t\t\t hash=" ++ fileHash
        else return ()

walkRecursive :: FilePath -> IO ()
walkRecursive path = do
    entries <- listDirectory path
    mapM_ (processEntry path) entries


commit :: [String] -> IO ()
commit [name, dir] = do
    setCurrentDirectory dir
    walkRecursive "."


dispatch :: [(String, [String] -> IO ())]
dispatch =  [ ("commit", commit)
            ]


main :: IO ()
main = do
    (command:args) <- getArgs
    let (Just action) = lookup command dispatch
    action args
