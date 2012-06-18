module Paths_cmq_haskell (
    version,
    getBinDir, getLibDir, getDataDir, getLibexecDir,
    getDataFileName
  ) where

import Data.Version (Version(..))
import System.Environment (getEnv)

version :: Version
version = Version {versionBranch = [0,0,1], versionTags = []}

bindir, libdir, datadir, libexecdir :: FilePath

bindir     = "/root/.cabal/bin"
libdir     = "/root/.cabal/lib/cmq-haskell-0.0.1/ghc-7.0.3"
datadir    = "/root/.cabal/share/cmq-haskell-0.0.1"
libexecdir = "/root/.cabal/libexec"

getBinDir, getLibDir, getDataDir, getLibexecDir :: IO FilePath
getBinDir = catch (getEnv "cmq_haskell_bindir") (\_ -> return bindir)
getLibDir = catch (getEnv "cmq_haskell_libdir") (\_ -> return libdir)
getDataDir = catch (getEnv "cmq_haskell_datadir") (\_ -> return datadir)
getLibexecDir = catch (getEnv "cmq_haskell_libexecdir") (\_ -> return libexecdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir ++ "/" ++ name)
