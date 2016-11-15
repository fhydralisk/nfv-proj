'''
Created on 2016-3-2

@author: hydra
'''

from HTTPRedirector import ProxyServerFactory
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ServerEndpoint
from src.redirector.RedirectorClientProtocol import RedirectorClientFactory
import os
class RedirectorRunner(object):
  
  def __init__(self, name, controllerAddr, bindAddr, serverPort = 80, serverInterface = ''):
    '''
    @param controllerAddr:(ip, controllerPort) of controller
    @param bindAddr: (ip, controllerPort) to bind to connec to controller 
    @param serverPort: controllerPort to run as a redirector server
    @param serverInterface: ip of interface to bind to listen
    '''
    self.name = name
    
    self.controllerAddr = controllerAddr
    self.bindAddr = bindAddr
    
    self.serverPort= serverPort
    self.serverInterface = serverInterface
    
    
  def runServer(self):
    self.serverFactory = ProxyServerFactory()
    self.proxyEndpoint = TCP4ServerEndpoint(reactor=reactor, port=self.serverPort, \
                                            interface=self.serverInterface)
    self.proxyEndpoint.listen(self.serverFactory)
  
  def runNode(self):
    
    self.clientFactory = RedirectorClientFactory(self.name, rdtable_path=os.path.expanduser('~') + 'rdTable.txt')
   
    reactor.connectTCP(self.controllerAddr[0], self.controllerAddr[1], self.clientFactory, 10, self.bindAddr)
    
    
  def run(self):
    reactor.run()
    


    