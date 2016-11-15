'''
Created on 2016-3-17

@author: hydra
'''
from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor
from twisted.internet.address import IPv4Address

class SProtocol(Protocol):
  def connectionMade(self):
    self.transport.abortConnection()
    Protocol.connectionMade(self)
  
  def connectionLost(self, reason):
    print reason
    Protocol.connectionLost(self, reason=reason)
    
class SFactory(Factory):
  protocol = SProtocol
  
  
def run():
  
  ep = TCP4ServerEndpoint(reactor, port=5555)
  ep.listen(SFactory())
  reactor.run()
  
  
run();