---------------------------------------------------------------------------
-- |
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
--
----------------------------------------------------------------------------

module System.CMQ (
                   -- * The queue identifier (Token)
                   Cmq
                   -- * IPv4 address of the destination node
                   , IPv4
                   -- * The destination identifier (KEY)
                   , KEY
                   -- * Construction
                   , newRq
                   -- * Insertion (Push Message)
                   , cwPush
                   -- * Query (Pop a Message)
                   , cwPop
) where

import Data.Time
import Data.Functor
import Data.Time.Clock.POSIX
import Data.List
import Data.String
import Data.Maybe
import Data.Serialize as S
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

type KEY = ( IPv4 , Integer ) -- ^ The 'KEY' identifies the message destination in the format 'IPv4' address, Integer.  
type TPSQ = TVar (PSQ.PSQ KEY POSIXTime)
type TMap a = TVar (Map.Map KEY [a]) 

-- | General purpose finite queue.
data Cmq a = Cmq { qthresh :: Int, tdelay :: Rational, cwpsq :: TPSQ, cwmap :: TMap a, cwchan :: TChan a }

getQthresh :: Reader (Cmq a) Int
getQthresh = do
   c <- ask
   return (qthresh c)

getDelay :: Reader (Cmq a) Rational
getDelay = do
   c <- ask
   return (tdelay c)

getTMap :: Reader (Cmq a) (TMap a)
getTMap = do
   c <- ask
   return (cwmap c)

getTPsq :: Reader (Cmq a) TPSQ
getTPsq = do
   c <- ask
   return (cwpsq c)

getTChan :: Reader (Cmq a) (TChan a)
getTChan = do
    c <- ask
    return (cwchan c)

-- | Builds and returns a new instance of Cmq.

newRq :: Serialize a => Socket -- ^ Socket does not need to be in connected state. 
         -> Int         -- ^ Maximum Queue length in bytes.
         -> Rational    -- ^ Maximum Queue age in ms.
         -> IO (Cmq a)  -- ^ Token returned to identify the Queue.
newRq s qthresh tdelay = do
      q <- atomically $ newTVar (PSQ.empty)
      m <- atomically $ newTVar (Map.empty)
      t <- newTChanIO
      let cmq = Cmq qthresh tdelay q m t
      forkIO $ loopMyQ s cmq q m
      forkIO $ loadTChan s t
      return cmq

loadTChan :: Serialize a => Socket -> TChan a -> IO ()
loadTChan s t = forever $ do
      (msg, _) <- receiveMessage s
      forkIO $ write2TChan msg t
 
appendMsg :: Serialize a => a -> KEY -> Cmq a -> TMap a -> IO Int
appendMsg newmsgs key cmq m =
      atomically $ do
         mT <- readTVar m
         messages' <- case (Map.lookup key mT) of
                           Nothing -> let messages' = [] in return messages'
                           _ -> let Just messages' = Map.lookup key mT in return messages'
         let l = B.length $ S.encode messages'
             l' = l + (B.length $ S.encode newmsgs)
         let env = runReader getQthresh cmq
         if l' < env then writeTVar m (Map.adjust (++ [newmsgs]) key mT) else writeTVar m mT
         return (env - l')

insertSglton :: a -> KEY -> TPSQ -> TMap a -> IO ()
insertSglton newmsgs key q m = do
      time <- getPOSIXTime
      atomically $ do
      qT <- readTVar q
      mT <- readTVar m
      writeTVar q (PSQ.insert key time qT)
      writeTVar m (Map.insert key [newmsgs] mT)
      return ()

-- | A message is pushed to CMQ. 

cwPush :: Serialize a => Socket -> KEY -> a -> Cmq a -> IO ()
cwPush s key newmsgs cmq = do
     now <- getPOSIXTime
     let m = runReader getTMap cmq
     let q = runReader getTPsq cmq
     qT <- atomically $ readTVar q
     case (PSQ.lookup key qT) of
          Nothing -> insertSglton newmsgs key q m
          _ -> do  result <- appendMsg newmsgs key cmq m
                   when (result <= 0) (transMit s now key newmsgs q m)

sendq s datastring host port = do
     hostAddr <- inet_addr host
     sendAllTo s datastring (SockAddrInet port hostAddr)

transMit :: Serialize a => Socket -> POSIXTime -> KEY -> a -> TPSQ -> TMap a -> IO ()
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
                                     Just messages -> sendq s (S.encode messages) (show a) 4711
     loopAction

transMit2 :: Serialize a => Socket -> POSIXTime -> KEY -> TPSQ -> TMap a -> IO ()
transMit2 s time key q m = do
     loopAction2 <- atomically $ do
                       mT <- readTVar m
                       qT <- readTVar q
                       let (a, _) = key
                       let mT' = Map.delete key mT
                       let qT' = PSQ.delete key qT
                       writeTVar q qT'
                       writeTVar m mT'
                       return (let Just messages = Map.lookup key mT in sendq s (S.encode messages) (show a)  4711)
     loopAction2

loopMyQ s cmq q m = forever $ do

      b <- atomically $ do q' <- readTVar q
                           case PSQ.findMin q' of
                              Just b  -> return b
                              Nothing -> retry

      let tdelay= runReader getDelay cmq
      let duetime = (PSQ.prio b) + (fromRational $ tdelay / 1000)
      let key = (PSQ.key b)
      now <- getPOSIXTime
      when (now > duetime) (transMit2 s now key q m)
      threadDelay 20 --this may need to be adjusted manually

write2TChan :: [a] -> TChan a -> IO ()
write2TChan msg mtch = do
        mapM_ (\x -> atomically $ writeTChan mtch x) msg
        return ()

-- | A message is popped of CMQ. The next value is read from the queue.

cwPop :: Cmq a -> IO (Maybe a)
cwPop cmq = do
        let mtch = runReader getTChan cmq
        let m = runReader getTMap cmq
        empty <- atomically $ isEmptyTChan mtch
        case empty of
             False -> do m <- atomically $ readTChan mtch
                         return (Just m)
             True -> return Nothing
        --Checks whether messages have arrived or not
        --before read is attempted. Makes the Pop non-blocking

receiveMessage :: (Serialize a) => Socket -> IO (a, SockAddr)
receiveMessage s  = do
        (msg, remoteSockAddr) <- recvFrom s 512 --influence on performance and best value still under investigation
        case S.decode msg of
             --Left str -> putStrLn str
             Right res -> return (res, remoteSockAddr)
