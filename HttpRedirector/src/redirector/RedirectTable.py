'''
Created on 

@author: hydra
'''

from src.lib.HitCountCurve import HitCountCurve
from src.lib.URLParser import URLParser

def singleton(cls):  
    instances = {}  
    def getinstance():  
        if cls not in instances:  
            instances[cls] = cls()  
        return instances[cls]  
    return getinstance  
 
@singleton  
class RedirectTable(object):
  '''
  classdocs
  '''
  peer_dict = {}
  time_granularity = 30
  time_hold = 5
  timestamp_ref = 0

  '''
  peer_dict : 
  {
    res (host, controllerPort, path) : (cachehost, cacheport, hitcountcurve)
  }
  '''

  def __init__(self):
    '''
    Constructor
    '''
    '''
    restest = ('static.youku.com', 80, '/index/img/header/yklogo_h.png')
    self.peer_dict[restest] = self.newEmptyPeerNode(restest)
    self.peer_dict[restest] = ('192.168.100.26', 80) + self.peer_dict[restest][2:3]
    '''
  
  def getPeerNode(self, host, port, res):
    '''
    Get suitable cache node of given host and res.
    @param host: remote host name, e.g. www.baidu.com, 6.6.6.6
    @param res: resource path, e.g. /index.html
    @param port: port, e.g. 80 
    @return: None - nocache, otherwise (ip, controllerPort)
    '''
    #TODO: wildcard match is not considered.
    '''
    if res[0]!='/':
      if not res.startswith('http://'):
        #maybe not http protocol?
        res += 'http://'
        pass
      else:
        res = URLParser(res).url_fields[2]
    '''
      
    resource = (host, port, res)
    tuple_node = None
    if not self.peer_dict.has_key(resource):
      # destination resource missing
      tuple_node = self.newEmptyPeerNode(resource)
      
      # init hit count curve
      self.peer_dict[resource] = tuple_node
      
      print "miss"
      
    else:
      tuple_node = self.peer_dict[resource]
      print "hit"
    
     
    #update hit count curve 
    hit_count = tuple_node[2]
    hit_count.hitNow()
    
    return tuple_node
  
  def updateRedirectTable(self, peers_update, flush = False):
    '''
    @param peers_update: a dictionary. { resource (host, controllerPort, res) : cache (host, controllerPort) }. if cache is None, remove this redirection.
    @param flush: if True, the peers_update will totally replace old redirect table. 
    '''
    
    if flush:
      self.peer_dict = {}

    for resource, cache in peers_update.items():
      modifiedCache = None
      if cache is None:
        if self.peer_dict.has_key(resource):
          modifiedCache = resource[0:2] + self.peer_dict[resource][2:3]
        #if no this key, ignore it.
      else:
        if self.peer_dict.has_key(resource):
          modifiedCache = cache + self.peer_dict[resource][2:3]
        else:
          peerNode = self.newEmptyPeerNode(resource)
          modifiedCache = cache + peerNode[2:3]
        
      if modifiedCache is not None:
        self.peer_dict[resource] = modifiedCache
  
  
  def newEmptyPeerNode(self, resource):
    '''
    @param resource: (host, controllerPort, res) tuple
    @return: (host, controllerPort, empty hitcount curve) 
    '''
    hit_count = HitCountCurve(time_granularity=self.time_granularity, time_hold=self.time_hold, timestamp_ref=self.timestamp_ref)
    return resource[0:2] + (hit_count, )
    
  def toJsonDict(self):
    return {'time_options' : (self.time_granularity, self.time_hold, self.timestamp_ref),
            'redirectTable' : [{'resource' : k, 'destination' : v[:2], 'hitcountcurve' : v[2].toJsonDict() }  
                               for k, v in self.peer_dict.items()]}
  
  def fromJsonDict(self, jsondict):
    self.time_granularity = jsondict['time_options'][0]
    self.time_hold = jsondict['time_options'][1]
    self.timestamp_ref = jsondict['time_options'][2]
    
    self.peer_dict = {}
    for l in jsondict['redirectTable']:
      self.peer_dict[tuple(l['resource'])] = tuple(l['destination'] + [HitCountCurve.fromJsonDict(l['hitcountcurve'])] )
    return self
    #print self.peer_dict
      
    
  

    