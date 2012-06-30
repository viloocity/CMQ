import CMQ
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Control.Monad
import Data.Maybe

main = withSocketsDo $ do
     qs <- socket AF_INET Datagram defaultProtocol
     hostAddr <- inet_addr "192.168.35.69"
     bindSocket qs (SockAddrInet 4711 hostAddr)
     token <- newRq qs 512 200
     forever $ do
        msg <- cwPop token :: IO (Maybe String)
        print msg 
