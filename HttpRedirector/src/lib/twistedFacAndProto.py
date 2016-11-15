'''
Created on 2016-3-1

@author: hydra
'''

from twisted.internet.protocol import Protocol, ReconnectingClientFactory

import json
from twisted.internet import task


class HyClientFactoryBase(ReconnectingClientFactory):
  
  def __init__(self, name, hb_interval = 15):
    self.name = name
    self.hb_interval = hb_interval
    self.maxDelay = 15
    
    
  def buildProtocol(self, addr):
    self.resetDelay()
    return ReconnectingClientFactory.buildProtocol(self, addr)
  
  def clientConnectionLost(self, connector, reason):
    print 'Lost connection.  Reason:', reason
    ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

  def clientConnectionFailed(self, connector, reason):
    print 'Connection failed. Reason:', reason
    ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
  
  
class HyClientProtocolBase(Protocol):
  
  '''
  name = None
  datapool = ''
  heartbeat_interval = 60  
  timer = None
  '''
  
  def __init__(self):
    #self.name = name
    self.datapool = ''
    #self.hb_interval = hb_interval
    
    
  def connectionLost(self, reason):
    #TODO: Reconnect shall take place
    print 'connection lost %s' % reason
    self.timer.stop()
    self.timer = None
    #if self.reconn:
      
    #  self.reconn.reconnect()
    
    Protocol.connectionLost(self, reason=reason)

  def connectionMade(self):
    addition = self.firstPackageAddition()
    
    firstPackage = {
                    'type':'name', 
                    'name':self.factory.name, 
                    'deviceType':self.factory.device_type, 
                    'internalAddress' : (self.transport.getHost().host, self.transport.getHost().port)
                    }
    
    if addition:
      firstPackage = dict(firstPackage, **addition)
      
    self.sendMessage(firstPackage)
    self.time = 0
    self.timer = task.LoopingCall(self.timerHit)
    self.timer.start(1, False)
    
    Protocol.connectionMade(self)
    
  def dataReceived(self, data):
    self.datapool = self.datapool + data

    
    packages = self.procceedData()
    
    for package in packages:
      try:
        pkgtype = package['type']
      except Exception, e:
        print e
        print "err: type excepted"
        continue
        
        #deliver package here to subclasses
      answer = self.packetReceived(package)
      if answer:
        cmdtype = self.getCmdType(package)
        extraAnswer = { 'reply' : cmdtype }
        
        self.sendAnwserMessage(pkgtype, package['commandid'], answer[0], answer[1], extraAnswer)
      
    
    Protocol.dataReceived(self, data)
  
    
  def packetReceived(self, package):
    pass
  
  def firstPackageAddition(self):
    return None
  
  def timerHit(self):
    self.time += 1
    if self.timeToCall(self.factory.hb_interval):
      self.sendHeartbeat()
    pass
  
  def timeToCall(self, interval):
    return self.time % interval == 0
  
  def sendHeartbeat(self, message = None):
    heartbeat = {'type' : 'heartbeat'}
    if message:
      heartbeat = dict(heartbeat, **message)
      
    self.sendMessage(heartbeat)

  
  def sendMessage(self, msgdict):
    try:
      self.transport.write(json.dumps(msgdict)+'\r\n')
      
    except TypeError:
      print 'error: message dict error'
  
  
  def sendAnwserMessage(self, cmdtype, commandid, message = None, error = None, addition = None):
    
    msg = {'type' : cmdtype + 'result', 'commandid' : commandid}
    
    #print message
    if message is not None:
      msg['result'] = message
    
    if error is not None:
      msg['error'] = error[0]
      msg['errordescription'] = error[1]
      
    if addition is not None:
      msg = dict(msg, **addition)
    
    #print msg
    self.sendMessage(msg)
  
    
    
  def procceedData(self):
    
    if len(self.datapool) <= 2:
      return
    
    messages = self.datapool.split('\r\n')
    msgcount = len(messages)
    
    #proceed tail message (maybe empty)
    self.datapool = messages[msgcount - 1]
    del messages[msgcount-1]
      
    
    packages = []
    
    for message in messages:
      try:
        jo = json.loads(message)
        packages.append(jo)
      except:
        print 'message parse failed'
      
    return packages
  
  @staticmethod
  def getCmdType(package):
    return package[package['type']]
  

