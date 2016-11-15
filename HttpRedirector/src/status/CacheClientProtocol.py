'''
Created on 2016-3-1

@author: hydra
'''

import commands, time

from src.lib.twistedFacAndProto import HyClientProtocolBase, HyClientFactoryBase
from src.lib.HTTPResponse import HTTPResponse
from src.lib.ErrorCode import ErrorCode
from SquidLogAnal import SquidLogAnal
from SquidStateAnal import SquidStateAnalyzer


class CacheClientProtocol(HyClientProtocolBase):
  
  overload = False
  state_last = None
  def firstPackageAddition(self):
    self.state_last = {
                       "status" : SquidStateAnalyzer().analyze(),
                       "time" : time.time()
                       }
                       
    return {'cachingPort' : 80, 'timeopt' : (self.factory.time_granularity, self.factory.time_hold, self.factory.timestamp_ref)}
  
  def packetReceived(self, package):
    
    if package['type'] == 'command':
      if package['command'] == 'addRes':
        return self.addResource(package)
      elif package['command'] == 'removeRes':
        return self.removeResource(package)
      elif package['command'] == 'updateRes':
        return self.updateResource(package)
      
    
    elif package['type'] == 'query':
      if package['query'] == 'status':
        return self.sendStatus(package)
      elif package['query'] == 'hitcountcurve':
        return self.sendHitCountCurve(package)
      elif package['query'] == 'resources':
        return self.sendResources(package)
      elif package['query'] == 'hitcountcurves':
        return self.sendHitCountCurves(package)
    
    HyClientProtocolBase.packetReceived(self, package)
  
  def sendStatus(self, package):
    #TODO: state field shall be convert here or in squid state analyzer.
    state = SquidStateAnalyzer().analyze()
    if len(state.keys()) == 0:
      state = None
      ecode = ErrorCode.error500_internalerror
    else:
      avgreq = self.getAvgRequestTillLast(state)
      stateAddition = {
                       'overloading' : avgreq > self.factory.requests_thres,
                       'requests per sec' : avgreq
                       }
      state = dict(state, **stateAddition)
      ecode = ErrorCode.error200_ok
      
    return (state, ecode)
  
  def sendHitCountCurve(self, package):
    curve2send = None
    try:
      anal = SquidLogAnal(self.factory.squid_log_path)
      #TODO: this will re-analyze the whole file, optimization shall be done here
      report = anal.analyze()
      #TODO: only GET method here, further consideration shall be taken here
      print package['res']
      curveobj = report[('GET', package['res'])]
      curve2send = curveobj.curve
      ecode = ErrorCode.error200_ok
    except KeyError:
      ecode = ErrorCode.error404_notfound
    except Exception, e:
      print e
      ecode = ErrorCode.error500_internalerror
      
    return (curve2send, ecode)
  
  def sendHitCountCurves(self, package):
    #curve2send = None

    anal = SquidLogAnal(self.factory.squid_log_path)
    #TODO: this will re-analyze the whole file, optimization shall be done here
    report = anal.analyze()
    #TODO: only GET method here, further consideration shall be taken here
    #print package['res']
    #curveobj = report[('GET', package['res'])]
    #curve2send = curveobj.curve
    
    curves = {k[1]:v.curve for k,v in report.items()}
    ecode = ErrorCode.error200_ok
    
    return (curves, ecode)

  
  def sendResources(self, package):
    res2send = None
    try:
      anal = SquidLogAnal(self.factory.squid_log_path)
      anal.time_granularity =self.factory.time_granularity
      anal.time_hold = self.factory.time_hold
      anal.timestamp_ref = self.factory.timestamp_ref
      #TODO: this will re-analyze the whole file, optimization shall be done here
      report = anal.analyze()
      #TODO: only GET method here, further consideration shall be taken here
      res2send = []
      for k in report.keys():
        res2send.append(k[1])
      ecode = ErrorCode.error200_ok
    except Exception, e:
      print e
      ecode = ErrorCode.error500_internalerror
      
    return (res2send, ecode)

    pass
  
  def addResource(self, package):
    (retval, outval) = commands.getstatusoutput('squidclient -m GET ' + package['res'])
    try:
      #print outval
      response = HTTPResponse(outval)
      #print response.response.status
      if response.response.status <> 200:
        ecode = ErrorCode.error404_notfound
      else:
        ecode = ErrorCode.error200_ok
    except Exception, e:
      print e
      ecode = ErrorCode.error400_badrequest
      
    return (None, ecode)
      
  
  def removeResource(self, package):
    (retval, outval) = commands.getstatusoutput('squidclient -m PURGE ' + package['res'])
    
    try:
      response = HTTPResponse(outval)
      if response.response.status <> 200:
        ecode = ErrorCode.error404_notfound
      else:
        ecode = ErrorCode.error200_ok
    except:
      ecode = ErrorCode.error400_badrequest
      
    return (None, ecode)

  
  def updateResource(self, package):
    #TODO: add update function here...
    pass
  
  def timerHit(self):
    HyClientProtocolBase.timerHit(self)
    if self.timeToCall(self.factory.state_get_interval):
      state = SquidStateAnalyzer().analyze()
      
      
      try:
        
        if self.getAvgRequestTillLast(state) > self.factory.requests_thres:
          #report event here
          #set overload flag. heartbeat is responsible for clearing it
          self.overload = True
      except KeyError:
        print 'client_http.requests not found'
        
      self.state_last = {'status':state, 'time':time.time()}
        
      
  def sendHeartbeat(self, message = None):
    if self.overload:
      #overload, send flag in heartbeat message
      self.overload = False
      flag = {'event':'overload'}
      
      if message:
        message = dict(message, **flag)
      else:
        message = flag
        
    HyClientProtocolBase.sendHeartbeat(self, message)
      
  def getAvgRequestTillLast(self, state):
    timenow = time.time()
    requestNow = state['Number of HTTP requests received']
    timelast = self.state_last['time']
    requestLast = self.state_last['status']['Number of HTTP requests received']
    if timenow == timelast:
      return 0
    
    return (requestNow - requestLast)/(timenow - timelast)
    
  
class CacheClientFactory(HyClientFactoryBase):
  protocol = CacheClientProtocol
  squid_log_path = '/var/log/squid/access.log'
  time_granularity = 1
  time_hold = 50
  timestamp_ref = 0
  device_type = 'cache'
  
  requests_thres = 0.5
  state_get_interval = 20
  
  

  
  
    