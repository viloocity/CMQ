----------------------------------------------------------------------------
-- demo_cqm_sender.hs demonstrator code how to use CMQ.
-- Copyright   : (c) 2012 Joerg Fritsch
--
-- License     : BSD-style
-- Maintainer  : J.Fritsch@cs.cardiff.ac.uk
-- Stability   : experimental
-- Portability : GHC
--
-- Sends 10000 UDP messages to two systems.
-----------------------------------------------------------------------------

{-# LANGUAGE OverloadedStrings #-}

import CMQ
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Control.Monad

main = withSocketsDo $ do
     qs <- socket AF_INET Datagram defaultProtocol
     hostAddr <- inet_addr "192.168.35.84"
     bindSocket qs (SockAddrInet 4711 hostAddr)
     (token) <- newRq qs 512 200 --initializes the queue with the desired parameters
     --qlength = 512B and max delay time in the queue is 200ms (minimum is 40ms)
     --token is the queue identifier where messages are sent to or poped off
     forM_ [0..10000] $ \i -> do
        cwPush qs ("192.168.35.69", 0) ("ping" :: String) token --send message "ping" to 
        --ipv4 address 192.168.35.69 using the queue specified in token
        cwPush qs ("192.168.35.85", 0) ("pong" :: String) token 
