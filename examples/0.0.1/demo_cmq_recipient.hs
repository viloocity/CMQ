----------------------------------------------------------------------------
-- demo_cqm_recipient.hs demonstrator code how to use CMQ.
-- Copyright   : (c) 2012 Joerg Fritsch
--
-- License     : BSD-style
-- Maintainer  : J.Fritsch@cs.cardiff.ac.uk
-- Stability   : experimental
-- Portability : GHC
--
-- Receives and prints UDP messages.
-- Messages can be shown on the screen w e.g. grep -v Nothing
-----------------------------------------------------------------------------

{-# LANGUAGE OverloadedStrings #-}

import CMQ
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Control.Monad
import Data.Maybe

main = withSocketsDo $ do
     qs <- socket AF_INET Datagram defaultProtocol
     hostAddr <- inet_addr "192.168.35.85"
     bindSocket qs (SockAddrInet 4711 hostAddr)
     (token) <- newRq qs 512 200
     forever $ do
        msg <- cwPop token
        print msg
