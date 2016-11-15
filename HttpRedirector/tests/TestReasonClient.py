'''
Created on 2016-3-17

@author: hydra
'''


from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ClientEndpoint,\
  connectProtocol
from twisted.internet import reactor
from twisted.internet.address import IPv4Address



class CProtocol(Protocol):
  def connectionMade(self):
    Protocol.connectionMade(self)
    
  def connectionLost(self, reason):
    print reason
    Protocol.connectionLost(self, reason=reason)


def run():
  
  print "client"
  point = TCP4ClientEndpoint(reactor, "127.0.0.1", 5555)
      #point = TCP4ClientEndpoint(reactor, peeraddr.host, 80)
  connectProtocol(point, CProtocol())
  
  reactor.run()
  
run()