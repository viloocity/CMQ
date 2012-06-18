---------------------------------------------------------------------------
-- CMQ - A lightweight, asynchronous high-performance messaging queue for
-- the cloud.
-- Copyright   : (c) 2012 Joerg Fritsch
--
-- License     : BSD-style
-- Maintainer  : J.Fritsch@cs.cardiff.ac.uk
-- Stability   : experimental
-- Portability : GHC
--
-- A message queue based on the UDP protocol.
----------------------------------------------------------------------------

module System.CMQ (newRq, cwPush, cwPop) where

import Data.Time
import Data.Functor
import Data.Time.Clock.POSIX
import Data.List
import Data.String
import Data.Maybe
import qualified Data.PSQueue as PSQ
import qualified Data.Map as Map
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TChan
import Control.Monad.State
import Control.Monad.Reader
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as B
import System.Timeout
import Data.IP

--PSQ = ( host , ID ) POSIXTime
--where the tuple is k and POSIXTime is p

--Map is ( host , ID ) [messages]
--where the tuple is the k and [messages] is a list of messages

--ID is an integer that is reserved for future use e.g. as unique process identifier

--qthresh = 1440 (MTU minus some overhead)
--qthresh = 512 (most commen UDP packet size e.g. DNS)

type KEY = (IPv4, Integer)
type TPSQ = TVar (PSQ.PSQ KEY POSIXTime)
type TMap = TVar (Map.Map KEY [String])

data Qcfg = Qcfg { qthresh :: Int, tdelay :: Rational, cwpsq :: TPSQ, cwmap :: TMap, cwchan :: TChan String }

getQthresh = do
   c <- ask
   return (qthresh c)

getDelay = do
   c <- ask
   return (tdelay c)

getTMap = do
   c <- ask
   return (cwmap c)

getTPsq = do
   c <- ask
   return (cwpsq c)

getTChan = do
    c <- ask
    return (cwchan c)

newRq :: Socket -> Int -> Rational -> IO (Qcfg)
newRq s qthresh tdelay = do
      q <- atomically $ newTVar (PSQ.empty)
      m <- atomically $ newTVar (Map.empty)
      t <- newTChanIO
      let qcfg = Qcfg qthresh tdelay q m t
      forkIO $ loopMyQ s qcfg q m
      forkIO $ loadTChan s t
      return (qcfg)

loadTChan :: Socket -> TChan String -> IO ()
loadTChan s t = forever $ do
      (msg, _) <- receiveMessage s
      forkIO $ write2TChan msg t
 
appendMsg :: String -> KEY -> Qcfg -> TMap -> IO Int
appendMsg newmsgs key qcfg m =
      atomically $ do
         mT <- readTVar m
         messages' <- case (Map.lookup key mT) of
                           Nothing -> let messages' = [] in return messages'
                           _ -> let Just messages' = Map.lookup key mT in return messages'
         let l = length . concat $ messages'
             l' = l + length newmsgs
         let env = runReader getQthresh qcfg
         if l' < env then writeTVar m (Map.adjust (++ [newmsgs]) key mT) else writeTVar m mT
         return (env - l')

insertSglton :: String -> KEY -> TPSQ -> TMap -> IO ()
insertSglton newmsgs key q m = do
      time <- getPOSIXTime
      atomically $ do
      qT <- readTVar q
      mT <- readTVar m
      writeTVar q (PSQ.insert key time qT)
      writeTVar m (Map.insert key (words newmsgs) mT)
      return ()

cwPush :: Socket -> KEY -> String -> Qcfg -> IO ()
cwPush s key newmsgs qcfg = do
     now <- getPOSIXTime
     let m = getTMap qcfg
     let q = getTPsq qcfg
     qT <- atomically $ readTVar q
     case (PSQ.lookup key qT) of
          Nothing -> insertSglton newmsgs key q m
          _ -> do  result <- appendMsg newmsgs key qcfg m
                   when (result <= 0) (transMit s now key newmsgs q m)

sendq :: Socket -> B.ByteString -> String -> PortNumber -> IO ()
sendq s datastring host port = do
     hostAddr <- inet_addr host
     sendAllTo s datastring (SockAddrInet port hostAddr)

transMit :: Socket -> POSIXTime -> KEY -> String -> TPSQ -> TMap -> IO ()
transMit s time key newmsgs q m = do
     loopAction <- atomically $ do
                       mT <- readTVar m
                       qT <- readTVar q
                       let (a, _) = key
                       let mT' = Map.delete key mT
                       let qT' = PSQ.delete key qT
                       writeTVar q (PSQ.insert key time qT')
                       writeTVar m (Map.insert key [newmsgs] mT')
                       return $ case Map.lookup key mT of
                                     Nothing -> return ()
                                     Just messages -> sendq s (B.pack $ unwords messages) (show a) 4711
     loopAction

transMit2 :: Socket -> POSIXTime -> KEY -> TPSQ -> TMap -> IO ()
transMit2 s time key q m = do
     loopAction2 <- atomically $ do
                       mT <- readTVar m
                       qT <- readTVar q
                       let (a, _) = key
                       let mT' = Map.delete key mT
                       let qT' = PSQ.delete key qT
                       writeTVar q qT'
                       writeTVar m mT'
                       return (let Just messages = Map.lookup key mT in sendq s (B.pack $ unwords messages) (show a)  4711)
     loopAction2

loopMyQ s qcfg q m = forever $ do

      b <- atomically $ do q' <- readTVar q
                           case PSQ.findMin q' of
                              Just b  -> return b
                              Nothing -> retry

      let tdelay= runReader getDelay qcfg
      let duetime = (PSQ.prio b) + (fromRational $ tdelay / 1000)
      let key = (PSQ.key b)
      now <- getPOSIXTime
      when (now > duetime) (transMit2 s now key q m)
      threadDelay 20 --this may need to be adjusted manually

write2TChan :: String -> TChan String -> IO ()
write2TChan msg mtch = do
        let mymessages = words msg
        mapM_ (\x -> atomically $ writeTChan mtch x) mymessages
        return ()

cwPop :: Qcfg -> IO (Maybe String)
cwPop qcfg = do
        let mtch = getTChan qcfg
        let m = getTMap qcfg
        empty <- atomically $ isEmptyTChan mtch
        case empty of
             False -> do m <- atomically $ readTChan mtch
                         return (Just m)
             True -> return Nothing
        --Checks whether messages have arrived or not
        --before read is attempted. Makes the Pop non-blocking

receiveMessage :: Socket -> IO (String, SockAddr)
receiveMessage s  = do
        (msg, remoteSockAddr) <- recvFrom s 512 --influence on performance and best value still under investigation
        return (B.unpack $ msg, remoteSockAddr)
