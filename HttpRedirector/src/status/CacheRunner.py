'''
Created on 2016-3-2

@author: hydra
'''


from twisted.internet import reactor
from CacheClientProtocol import CacheClientFactory


class CacheRunner(object):
  
  def __init__(self, name, controllerAddr, bindAddr, pathLog):
    '''
    @param controllerAddr:(ip, port) of controller
    @param bindAddr: (ip, port) to bind to connec to controller 
    @param serverPort: port to run as a redirector server
    @param serverInterface: ip of interface to bind to listen
    '''
    self.controllerAddr = controllerAddr
    self.bindAddr = bindAddr
    self.name = name
    self.pathLog = pathLog
    
  
  def runNode(self):
    
    self.clientFactory = CacheClientFactory(self.name)
    if self.pathLog:
      self.clientFactory.squid_log_path = self.pathLog
    #self.clientPoint = TCP4ClientEndpoint(reactor = reactor, host = self.controllerAddr[0], \
    #                                       port = self.controllerAddr[1], bindAddress=self.bindAddr)
    
    #connectProtocol(self.clientPoint, self.clientProtocol)
    reactor.connectTCP(self.controllerAddr[0], self.controllerAddr[1], self.clientFactory, 10, self.bindAddr)
    
    
  def run(self):
    reactor.run()
    

