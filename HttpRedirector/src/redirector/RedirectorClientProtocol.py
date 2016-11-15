'''
Created on 2016-3-1

@author: hydra
'''

from src.lib.twistedFacAndProto import HyClientProtocolBase, HyClientFactoryBase
from RedirectTable import RedirectTable
from src.lib.URLParser import URLParser
from src.lib.ErrorCode import ErrorCode
import json

class RedirectorClientProtocol(HyClientProtocolBase):
  #redirect_table = RedirectTable()
  #remote_server_port_default = 80
  #cache_server_port_default = 80

  
  def packetReceived(self, package):
    #answer = None
    if package['type'] == 'command':
      if package['command'] == 'updateRedirectTable':
        return self.updateRedirectTable(package)
        #self.sendAnwserMessage(package['type'], package['commandid'], None, ErrorCode.error200_ok)
      pass
    
    elif package['type'] == 'query':
      if package['query'] == 'status':
        return self.sendStatus(package)
      elif package['query'] == 'hitcountcurve':
        return self.sendHitCountCurve(package)
      elif package['query']=='redirectTable':
        return self.sendRedirectTable(package)
        
    #if answer:
    #  self.sendAnwserMessage(package['type'], package['commandid'], answer[0], answer[1])
      
    HyClientProtocolBase.packetReceived(self, package)
    
  def sendStatus(self, package):
    #TODO: what status to send?
    return (None, ErrorCode.error501_notimplemented)
  
  def sendHitCountCurve(self, package):
    try:
      resource = URLParser(package['res'], self.factory.remote_server_port_default).url_fields
      hitcountcurve = self.factory.redirect_table.peer_dict[resource][2]
      #print self.factory.redirect_table.peer_dict
      #print hitcountcurve
      #print hitcountcurve.curve
      #self.sendAnwserMessage(package['type'], package['commandid'], hitcountcurve.curve, ErrorCode.error200_ok)
      return (hitcountcurve.curve, ErrorCode.error200_ok)
    except ValueError:
      #raise by urlparser
      return (None, ErrorCode.error400_badrequest)
    except KeyError:
      #raise in try
      return (None, ErrorCode.error404_notfound)        
    
    
  
  def sendRedirectTable(self, package):
    tblToSend = {}
    for k, v in self.factory.redirect_table.peer_dict.items():
      url = 'http://' + k[0] + ':' + str(k[1]) + k[2]
      cache = v[0] + ':' + str(v[1])
      tblToSend[url] = cache
    
    return (tblToSend, ErrorCode.error200_ok)
    
  
  
  def updateRedirectTable(self, package):
    if self.factory.redirect_table is None:
      print 'redirect table is null'
      return (None, ErrorCode.error500_internalerror)
    
    rdTable = package['table']
    #conversion
    
    rdTableConv = {}
    
    for k, v in rdTable.items():
      try:
        resource = URLParser(k, self.factory.remote_server_port_default).url_fields
      except ValueError:
        continue
      
      cache = v.split(':')
      cachehost = cache[0]
      if len(cache) == 2:
        cacheport = int(cache[1])
      else:
        cacheport = self.factory.cache_server_port_default
        
      cache = (cachehost, cacheport)
        
      rdTableConv[resource] = cache
      
      self.factory.updateRedirectTable(rdTableConv, package['flush']<>0)
    return (None, ErrorCode.error200_ok)
    
    
class RedirectorClientFactory(HyClientFactoryBase):
  remote_server_port_default = 80
  cache_server_port_default = 80
  protocol = RedirectorClientProtocol
  device_type = 'redirector'
  
  def __init__(self, name, hb_interval = 15, rdtable_path = None):
    self.rdtable_path = rdtable_path
    self.redirect_table = RedirectTable()
    if (self.rdtable_path):
      try:
        fp = open(self.rdtable_path, 'r')
        try:
          rdTableJson = json.load(fp)
          self.redirect_table.fromJsonDict(rdTableJson)
        except Exception, e:
          print e
        finally:
          fp.close()
      except:
        pass

    HyClientFactoryBase.__init__(self, name, hb_interval)
    
  def updateRedirectTable(self, rdTbl, flush):
    self.redirect_table.updateRedirectTable(rdTbl, flush)
    jdict = self.redirect_table.toJsonDict()
    if (self.rdtable_path):
      try:
        fp = open(self.rdtable_path, 'w')
        try:
          json.dump(jdict, fp)
        except Exception, e:
          print e
        finally:
          fp.close()
      except Exception, e:
        print e
        
  
  