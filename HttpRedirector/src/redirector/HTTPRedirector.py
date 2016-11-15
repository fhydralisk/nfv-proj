'''
Created on

@author: hydra
'''

from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ClientEndpoint,\
  connectProtocol
from twisted.internet import reactor
from twisted.internet.address import IPv4Address


from src.lib.HTTPRequest import HTTPRequest
from src.redirector.RedirectTable import RedirectTable
from src.lib.URLParser import URLParser

class ProxyPeer(Protocol):
  
  def __init__(self, peer):
    self.peerpeer = None
    self.forwarddata = None
    self.closed_by_self = False
    
    self.peerpeer = peer
    
  def dataReceived(self, data):
    
    if self.peerpeer and self.peerpeer.connected:
      self.peerpeer.transport.write(data)
    else:
      print 'peer not connected'
      self.transport.abortConnection()
    
    Protocol.dataReceived(self, data)
    
  def forward(self, data):
    if self.peerpeer is None:
      #connection is lost
      return
    
    if self.connected:
      if self.forwarddata:
        self.transport.write(self.forwarddata)
        self.forwarddata = None
        
      self.transport.write(data)
    else:
      if self.forwarddata is None:
        self.forwarddata = data
      else:
        self.forwarddata = self.forwarddata + data
  
  def connectionMade(self):
    print "peer connected"
    if self.forwarddata:
      self.transport.write(self.forwarddata)
      self.forwarddata = None
      
    Protocol.connectionMade(self)
  
  def connectionLost(self, reason):
    print "peer disconnected"
    if self.closed_by_self == False and self.peerpeer is not None:
      self.peerpeer.transport.loseConnection()
      
    self.peerpeer = None
    Protocol.connectionLost(self, reason=reason)
    
  def closeConnection(self, force = False):
    self.closed_by_self = True
    if force:
      self.transport.abortConnection()
    else:
      self.transport.loseConnection()


class ProxyServer(Protocol):
  
  orig_host = None
  orig_hostaddr = None
  peerpoint = None
  
  def connectionMade(self):
    #print self.transport.getPeer()
    print "ClientConnected"
    Protocol.connectionMade(self)
    
  def connectionLost(self, reason):
    print "Client disconnected"
    if self.peerpoint and self.peerpoint.connected:
      self.peerpoint.transport.abortConnection()
      
    Protocol.connectionLost(self, reason=reason)
    
  def dataReceived(self, data):
    '''
    Proxy received packet from client.
    '''
    
    #get http request information from packet if it is http protocol...
    request = HTTPRequest(data)
    if request.error_code <> 0:
      #shall be next packet?
      pass
    res = request.path
    #res may be: None, /xxx, www.baidu.com/xxx, http://www.baidu.com/xxx
    if res[0] == '/':
      pass
    elif res.startswith('http://'):
      res = URLParser(res).parse()[2]
      if res is None:
        res = '/'
    elif res == "":
      res = '/'
    else:
      try:
        result = URLParser("http://" + res).parse()
        res = result[2]
        if res is None:
          res = '/'
      except ValueError:
        self.transport.abortConnection()
      
    
    host = request.headers['host'].split(":")[0]
    if self.orig_host:
      if self.orig_host <> host:
        print 'host changed...'
        self.orig_host = host
        self.orig_hostaddr = IPv4Address(host = host, port = self.transport.getHost().port, type = 'TCP')
    else:
      self.orig_host = host
      self.orig_hostaddr = IPv4Address(host = host, port = self.transport.getHost().port, type = 'TCP')
      print host
      
      
    #print host
    #get cache node
    peernode = self.factory.redirectTable.getPeerNode(host = self.orig_host, res = res, port = self.transport.getHost().port)
    peeraddr = IPv4Address(host=peernode[0], port=peernode[1], type='TCP')
    
    #check if this cache node is same as last one.
    if (self.peerpoint is None) or (not self.peerpoint.connected) or (self.peerpoint.transport.getPeer() <> peeraddr):
      #no it's not
      if self.peerpoint and self.peerpoint.connected:
        #if connecting is opening, close it first
        self.peerpoint.closeConnection()
      
      #create new node of the selected caches
      self.peerpoint = ProxyPeer(self)
      point = TCP4ClientEndpoint(reactor, peeraddr.host, peeraddr.port)
      #point = TCP4ClientEndpoint(reactor, peeraddr.host, 80)
      connectProtocol(point, self.peerpoint)
      
      #print peeraddr
    
     
    print "forward" + str((host, self.transport.getHost().port, res)) + "-->" + str(peeraddr)
    
    if peeraddr != self.orig_hostaddr:
      print "redirect to cache, modifying request"
      print request.command
      data = request.string_by_adding_proxy_header(peeraddr.port)
      print data
    else:
      data = request.string_by_removing_proxy_header(peeraddr.port)
      
      
    self.peerpoint.forward(data)
    
    #print data

    Protocol.dataReceived(self, data)
  

class ProxyServerFactory(Factory):
  protocol = ProxyServer
  redirectTable = RedirectTable()
  def __init__(self):
    pass
  

