'''
Created on 2016

@author: hydra
'''

from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor, task

import threading
import json
from twisted.internet.address import IPv4Address
import httplib2


class HyFactoryBase(Factory):
  dictlock = threading.RLock()
  #client_dict = {}
  '''
  client_dict: { 
                client_name : {pobj:protocol , hearbeat:hb, ...} , ...}
  '''
  '''
  timer = None
  
  timeout_dead = 60
  timeout_kill = 120
  '''
  field_hb = 'heartbeat'
  field_protocol = 'pobj'
  
  
  def __init__(self, timeout_dead = 60, timeout_kill = 120, timeout_query = 10, server = None):
    #self.timer = threading.Timer(timer_interval, self.timerHit, [self])
    self.timeout_dead = timeout_dead
    self.timeout_kill = timeout_kill
    self.timeout_query = timeout_query
    self.time = 0
    self.server = server
    #self.timer.run()
    
    self.timer = task.LoopingCall(self.timerHit)
    self.timer.start(1, False)
    self.client_dict = {}
    self.commandid = 0
    '''
    queries:
    {
      commandid : {event : qevent_object, timer : timer},...
    }
    '''
    self.queries = {}
    
  def getProtocol(self, name):
    return self.client_dict[name][HyFactoryBase.field_protocol]
    
  
  def timerHit(self):
      
    self.time += 1
    self.dictlock.acquire()
    
    for k, v in self.client_dict.items():
      v[self.field_hb] = v[self.field_hb] + 1
      if v[self.field_hb] > self.timeout_kill:
        #timeout kill
        if v[self.field_protocol].connected:
          v[self.field_protocol].transport.abortConnection()
      
        del self.client_dict[k]
        
    for k, v in self.queries.items():
      v['timer'] = v['timer'] + 1
      if v['timer'] > self.timeout_query:
        self.setResultEvent(k, False)
        
    #print self.queries
        
        
    self.dictlock.release()
    
  def timeToCall(self, interval):
    return self.time % interval == 0
  
  def getCommandId(self):
    commandid = self.commandid
    self.commandid += 1
    return commandid

    
  def addClientNode(self, node):
    self.dictlock.acquire()
    
    if not self.client_dict.has_key(node.name):
      self.client_dict[node.name] = {}
      
    self.client_dict[node.name][self.field_protocol] = node
    self.client_dict[node.name][self.field_hb] = 0
    
    print self.client_dict
    self.dictlock.release()
    
  def removeClientNode(self, node):
    try:
      if (self.client_dict[node.name][self.field_protocol] is node):
        del self.client_dict[node.name]
    except KeyError:
      pass
    
  def heartbeat(self, node):
    self.dictlock.acquire()
    self.client_dict[node.name][self.field_hb] = 0
    self.dictlock.release()
    
  def appendClientField(self, node, key, value):
    self.dictlock.acquire()
    try:
      self.client_dict[node.name][key] = value
    except KeyError:
      pass
    
    self.dictlock.release()
    
  def stopFactory(self):
    self.timer.stop()
    Factory.stopFactory(self)
    
  def addResultEvent(self, commandid, qevent):
    self.queries[commandid] = {'event' : qevent, 'timer' : 0}
    
  
  def setResultEvent(self, commandid, result=False, package = None):
    if self.queries.has_key(commandid):
      self.queries[commandid]['event'].result = result
      if package:
        self.queries[commandid]['event'].package = package
      self.queries[commandid]['event'].set()
      del self.queries[commandid]
      
  
  def syncActiveMessage(self, name, call, *args, **kwargs):
    if name not in self.client_dict.keys():
      raise NameError
    
    qevent = threading.Event()
    qevent.clear()
    qevent.result = False
    
    node = self.client_dict[name][HyFactoryBase.field_protocol]
    kwargs['qevent'] = qevent
    kwargs['wait'] = True
    getattr(node, call)(*args, **kwargs)
    if qevent.result:
      return qevent.package
    
    return None
  
  def syncActiveGroupMessage(self, commandlist):
    qevents = []
    for unit in commandlist:
      name = unit['name']
      if name not in self.client_dict.keys():
        continue
      
      call = unit['call']
      
      qevent = threading.Event()
      qevent.clear()
      qevent.result = False
      qevents.append((name, call, qevent))
      
      node = self.client_dict[name][HyFactoryBase.field_protocol]
      
      try:
        args = unit['args']
      except KeyError:
        args = []
        
      try:
        kwargs = unit['kwargs']
      except KeyError:
        kwargs = {}
      
      kwargs['qevent'] = qevent
      kwargs['wait'] = False
      getattr(node, call)(*args, **kwargs)
      
    retval = []
    #print self.queries
    #print threading.current_thread()
    for name, call, qevent in qevents:
      qevent.wait()
      if qevent.result:
        retval.append({'name':name, 'call':call, 'package':qevent.package})
    
    return retval
  
  
  def getClientAddress(self, name, internal_addr = True):
    if internal_addr:
      internal_addr = self.client_dict[name][HyFactoryBase.field_protocol].internal_address
      return {'ip': internal_addr.host, 'port' : internal_addr.port}
    else:
      return self.client_dict[name][HyFactoryBase.field_protocol].getAddress()
      
    

class HyProtocolBase(Protocol):
  
  def __init__(self):
    self.name = None
    self.datapool = ''
    self.first_package = True
  
  def connectionLost(self, reason):
    try:
      self.factory.removeClientNode(self)
    except:
      print 'duplicate client?'
      
    Protocol.connectionLost(self, reason=reason)

  def connectionMade(self):
    Protocol.connectionMade(self)
    
  def dataReceived(self, data):
    
    #print threading.current_thread()
    self.datapool = self.datapool + data

    
    packages = self.procceedData()
    
    for package in packages:
      try:
        pkgtype = package['type']
      except Exception, e:
        print e
        print "err: type excepted"
        continue
        
      if self.first_package:
        self.first_package = False
        
        if pkgtype <> 'name':
          self.transport.abortConnection()
          print 'first packet type error'
          return
        try:
          self.firstPackage(package)
          
        except KeyError:
          print 'first package parse failed'
          self.transport.abortConnection()
          return
      else:
        if pkgtype == self.factory.field_hb:
          #TODO: should check if the object is me
          
          #clear timeout due to heartbeat
          
          self.factory.heartbeat(self)
          if package.has_key('event'):
            self.eventReceived(package['event'])
          
        else:
          #deliver package here to subclasses
          if pkgtype in ('queryresult' , 'commandresult'):
            #hit events here
            self.factory.setResultEvent(package['commandid'], True, package)
            
          self.packetReceived(package)
        pass
    
    Protocol.dataReceived(self, data)
    
  def packetReceived(self, package):
    pass
  
  def eventReceived(self, revent):
    pass
  
  def firstPackage(self, package):
    self.name = package['name']
    self.internal_address = IPv4Address(host=package['internalAddress'][0], port=package['internalAddress'][1], type='TCP')
    self.factory.addClientNode(self)
    if package['deviceType']<>self.factory.device_type:
      print 'device type error:' + str(self.transport.getPeer())
      print 'expect ' + self.factory.device_type + ', but ' + package['deviceType']
      self.transport.abortConnection()
    

  
  def sendMessage(self, msgdict):
    try:
      #TODO: make it thread safe.
      self.transport.write(json.dumps(msgdict)+'\r\n')
      
    except TypeError:
      print 'error: message dict error'
  
  def sendCommand(self, command, message = None, qevent = None, wait = False):
    return self.sendActiveMessage('command', command, message, qevent, wait)
    
  def sendQuery(self, query, message = None, qevent = None, wait = False):
    #print 'query %s' % self.name
    #print self.factory.client_dict
    return self.sendActiveMessage('query', query, message, qevent, wait)
    
  def sendActiveMessage(self, pkgtype, cmd, message, qevent, wait):
    package = {'type' : pkgtype, pkgtype : cmd}
    package['commandid'] = self.factory.getCommandId()
    
    if message:
      for k, v in message.items():
        package[k] = v
    
    if qevent:
      self.factory.addResultEvent(package['commandid'], qevent)
      
    self.sendMessage(package)
    
    if wait:
      if qevent is None:
        raise ValueError
      
      qevent.wait()
      
      return qevent.result
    
    return None

    
  def procceedData(self):
    
    if len(self.datapool) <=2:
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
  
  def getAddress(self):
    addr = self.transport.getPeer()
    return {'ip': addr.host, 'port': addr.port}

class RedirectorProtocol(HyProtocolBase):
  def __init__(self):
    HyProtocolBase.__init__(self)
    
  def packetReceived(self, package):
    pkgtype = package['type']
    if pkgtype == 'queryresult':
      try:
        querytype = package['reply']
      except KeyError:
        print 'received an answer packet without "reply" key.'
        return
      
      if querytype == 'status':
        self.factory.appendClientField(self, 'status', package['result'])


  def updateStatus(self, status):
    #self.factory.client_dict[self.name]['status'] = status
    self.factory.appendClientField(self, 'status', status)
    
  def queryStatus(self, qevent = None, wait = False):
    return self.sendQuery('status', None, qevent, wait)
  
  def pushRedirectTable(self, tbl, flush = 1, qevent = None, wait = False):
    return self.sendCommand('updateRedirectTable', {'table' : tbl, 'flush' : flush}, qevent, wait)
    
  def queryRedirectTable(self, qevent = None, wait = False):
    return self.sendQuery('redirectTable', None, qevent, wait)
  
  def queryHitCountCurve(self, url, qevent = None, wait = False):
    return self.sendQuery('hitcountcurve', {'res' : url}, qevent, wait)
  

    
  
class RedirectorFactory(HyFactoryBase):
  protocol = RedirectorProtocol
  device_type = 'redirector'
  
  def __init__(self, timeout_dead = 60, timeout_kill = 120, timeout_query = 10, server = None):
    HyFactoryBase.__init__(self, timeout_dead, timeout_kill, timeout_query, server)
    
  def queryStatus(self, name):
    return self.syncActiveMessage(name, 'queryStatus')
  
  def queryRedirectTable(self, name):
    return self.syncActiveMessage(name, 'queryRedirectTable')
  
  def queryHitCountCurve(self, name, url):
    return self.syncActiveMessage(name, 'queryHitCountCurve', url)
  
  def pushRedirectTable(self, name, tbl, flush = 1):
    return self.syncActiveMessage(name, 'pushRedirectTable', tbl, flush)



class CacheProtocol(HyProtocolBase):
  def __init__(self):
    self.overload = False
    HyProtocolBase.__init__(self)
    
  
  def firstPackage(self, package):
    HyProtocolBase.firstPackage(self, package)
    self.caching_port = package['cachingPort']
    self.time_granularity = package['timeopt'][0]
    self.time_hold = package['timeopt'][1]
    self.timestamp_ref = package['timeopt'][2]

  
  def packetReceived(self, package):
    '''
    try:
      if package['type'] == 'status':
        self.updateStatus(package['status'])
      elif package['type'] == 'hitcountcurve':
        pass

    except:
      print "package parsing error"
    '''
    pkgtype = package['type']
    if pkgtype == 'queryresult':
      try:
        querytype = package['reply']
      except KeyError:
        print 'received an answer packet without "reply" key.'
        return
      
      if querytype == 'status':
        self.factory.appendClientField(self, 'status', package['result'])
        
      

  
  def eventReceived(self, revent):
    if revent == 'overload':
      #overload event processing
      print "Event: overload. cache: %s" % self.name
      self.overload = True
      
    HyProtocolBase.eventReceived(self, revent)
    
  def sendCachingCommand(self, url, command='addRes', qevent = None, wait = None):
    self.sendCommand(command, {'res' : url}, qevent, wait)
    
    #TODO: how to 'return' the message? threading consideration should be taken...
  def queryStatus(self, qevent = None, wait = False):
    self.sendQuery('status', None, qevent, wait)
    
  def queryResources(self, qevent = None, wait = False):
    self.sendQuery('resources', None, qevent, wait)
    
  def queryHitCountCurve(self, url, qevent = None, wait = False):
    self.sendQuery('hitcountcurve', {'res' : url}, qevent, wait)
    
  def queryHitCountCurves(self, qevent = None, wait = False):
    self.sendQuery('hitcountcurves', None, qevent, wait)
    


class CacheFactory(HyFactoryBase):
  protocol = CacheProtocol
  interval_checkstatus = 30
  report_thres = 0.5
  device_type = 'cache'
  curve_count_sum_min = 10
  
  def __init__(self, timeout_dead = 60, timeout_kill = 120, timeout_query = 10, server = None):
    HyFactoryBase.__init__(self, timeout_dead, timeout_kill, timeout_query, server)
    
  def queryStatus(self, name):
    return self.syncActiveMessage(name, 'queryStatus')
  
  def queryResources(self, name):
    return self.syncActiveMessage(name, 'queryResources')
  
  def queryHitCountCurve(self, name, url):
    return self.syncActiveMessage(name, 'queryHitCountCurve', url)
  
  def sendCachingCommand(self, name, url, command):
    return self.syncActiveMessage(name, 'sendCachingCommand', url = url, command = command)
  
  def queryResourcesOfAll(self):
    name2query = [{'name': name, 'call' : 'queryResources'} for name in self.client_dict.keys()]
    result = self.syncActiveGroupMessage(name2query)
    return result
  
  def timerHit(self):
    if self.timeToCall(self.interval_checkstatus):
      checkingThread = threading.Thread(target=self.checkStatus)
      checkingThread.setDaemon(True)
      checkingThread.start()

        
    HyFactoryBase.timerHit(self)
    
  def checkStatus(self):
    if len(self.client_dict.keys()) > 0:
      overloads = [cache[self.field_protocol] for cache in self.client_dict.values() if cache[self.field_protocol].overload]
      if float(len(overloads)) / float(len(self.client_dict.keys())) > self.report_thres: 
        self.server.reportToCoordinator(self, 'overload')
      
      hccsQuery = [{'name':cache.name, 'call' : 'queryHitCountCurves'} for cache in overloads]
      if hccsQuery:
        hccs = self.syncActiveGroupMessage(hccsQuery)
        
        #call server to handle and proceed the overload event
        self.server.proceedCacheOverload(hccs)
        
      for overload in overloads:
        #clear flags
        overload.overload = False
      
    


class ControlServer(object):
  '''
  classdocs
  '''
  
  ports = (6058, 6059)
  ip = ''
  
  detach_thread = None
  ControllerEndpointCache = None
  ControllerEndpointRedirector = None
  cache_curve_count_to_learn_in_min = 10


  def __init__(self, ip='', ports=(6058, 6059)):
    '''
    @param ip: ip to bind and listen
    @param ports:a tuple of port to listen, first of which is cache's port and second is redirector's
    '''
    
    self.ports = ports
    self.ip = ip
    self.cacheFactory = CacheFactory(server = self)
    self.redirectorFactory = RedirectorFactory(server = self)
    self.profile = None
    self.resturi = "/restconf/operations/event:push-event/"
    self.restaddr = '166.111.65.110'
    self.restport = 8181
    self.restuser = 'admin'
    self.restpwd = 'admin'
    
    
    pass
  
  def run(self):
    if self.profile.has_key('cache_curve_count_to_learn_in_min'):
      self.cache_curve_count_to_learn_in_min = self.profile['cache_curve_count_to_learn_in_min']
    
    if self.profile.has_key('uri_client_coordinator_rest'):
      self.resturi = self.profile['uri_client_coordinator_rest']
      
    if self.profile.has_key('username_client_coordinator_rest'):
      self.restuser = self.profile['username_client_coordinator_rest']
      
    if self.profile.has_key('password_client_coordinator_rest'):
      self.restpwd = self.profile['password_client_coordinator_rest']
      
    self.restaddr = self.profile['addr_client_coordinator']
    self.restport = self.profile['port_client_coordinator']
    
    print 'trying to start manage server...'
    
    if (self.ControllerEndpointCache is not None or self.ControllerEndpointRedirector is not None):
      print "err: trying to rerun controller manage server"
      return
    
    try:
      self.ControllerEndpointCache = TCP4ServerEndpoint(reactor = reactor, port = self.ports[0], interface = self.ip)
      self.ControllerEndpointRedirector = TCP4ServerEndpoint(reactor = reactor, port = self.ports[1], interface = self.ip)
      self.ControllerEndpointCache.listen(self.cacheFactory)
      
      print 'cache server start listening'
      self.ControllerEndpointRedirector.listen(self.redirectorFactory)
      print 'redirector server start listening'
    except:
      print "err: listen() failed, check port usage"
      
      
    reactor.run()
    
    print 'exiting manage server...'
    
    self.detach_thread = None
    
  
  
  def runAsync(self):
    '''
    
    '''
    
    if self.detach_thread is not None:
      print "err: trying to rerun controller manage server"
      return
    
    self.detach_thread = threading.Thread(target = self.run)
    self.detach_thread.setDaemon(True)
    self.detach_thread.start()
    
  
  
  def stop(self):
    if self.detach_thread is None:
      return
    
    print 'trying to stop server'
    
    reactor.stop()
    
    self.detach_thread.join()
    self.detach_thread = None
    self.ControllerEndpointCache = None
    self.ControllerEndpointRedirector = None
    
    print 'server terminated'
    
  def recalcRedirectTable(self):
    pass
  
  def reportToCoordinator(self, caller, event):
    if event == 'overload':
      eventToReport = '''
{
    "input":{
        "name":"event1";
        "event-type":"cache",
        "event-detail":[
            {
                "name":"detail1",
                "description":"NEW"
            }]
    }
}'''
      try:
        
        httplib2.debuglevel = 1
        h = httplib2.Http()
        h.add_credentials(self.restuser, self.restpwd)
        response, content = h.request('http://%s:%s%s'%(self.restaddr, self.restport, self.resturi), 'POST', 
                                      eventToReport,headers={'Content-Type':'application/json', 'Accept':'*/*'})
        print "reporting to coordinator"
        print response
        print content
        
      except:
        print 'connection to coordinator failed'

  
  
  def proceedCacheOverload(self, hccs):
    #recal redirect table here
    caches = self.cacheFactory.client_dict.keys()
    overloadCaches = [n['name'] for n in hccs]
    
    normalCaches = [c for c in caches if c not in overloadCaches]
    
    if normalCaches:
      #calc a list of max hits of each overloaded cache
      theList = []
      
      for hcc in hccs:
        name = hcc['name']
        payload = hcc['package']
        maxhits = 0
        maxRecord = None
        curveCountToSum = self.getCurveCountToSum(name)
        for res, hitcountcurve in payload['result'].items():
          hitcount = 0
          if not res.startswith('http://'):
            print 'ignoring %s' % res
            continue
          
          for count in hitcountcurve[-curveCountToSum:]:
            hitcount += count['count']
            
          if hitcount > maxhits:
            maxRecord = (name, res, hitcount) #node of the list
            maxhits = hitcount
            pass
          pass
        
        
        if maxRecord:
          print "find a max record of %s:%s" % (name, maxRecord)
          theList.append(maxRecord)
          
      theList.sort(key=lambda k:k[2], reverse=True)
      res2move = theList[:len(normalCaches)]
      print theList
      #TODO: move res here. the algorithm must be modified here. but this is just a demo
      tblsToPush = {}
      for i in range(len(res2move)):
        cache_move2 = self.cacheFactory.getProtocol(normalCaches[i])
        addrOfNormalCache = cache_move2.internal_address
        
        #TODO: cache port default 80, just a demo, should be fix!
        #TODO: this is a trick, using internal address.. how to solve it?
        
        #res2move[i]: node; node[1]:res; res[1]:url
        tblsToPush[res2move[i][1]] = addrOfNormalCache.host + ':' + str(cache_move2.caching_port)
        
      
      print "trying to push redirect table to all redirectors:"
      print tblsToPush
      #push the redirect table to each of redirectors. demo
      for redirector in self.redirectorFactory.client_dict.keys():
        self.redirectorFactory.client_dict[redirector][CacheFactory.field_protocol].pushRedirectTable(tblsToPush, 0)
        
  def getCurveCountToSum(self, cachename):
    cachenode = self.cacheFactory.getProtocol(cachename)
    granularity = cachenode.time_granularity
    return (self.cache_curve_count_to_learn_in_min / granularity + 1)
        