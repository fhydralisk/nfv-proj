'''
Created on 2016-3-3

@author: hydra
'''

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import threading, json, traceback
import base64
from __builtin__ import True

class ControllerHTTPHandler(BaseHTTPRequestHandler):
  #resources = { ('GET', '/caches') : ''}
  
  factoryRedirector = None
  factoryCache = None
  
  #def __init__(self, request, client_address, server):
  #  BaseHTTPRequestHandler.__init__(self, request, client_address, server)
  
  def writeCommonHeader(self, contenttype):
    self.protocol_version = 'HTTP/1.1'
    self.send_response(200)
    self.send_header('Content-Type', contenttype)
    self.end_headers()

  
  def do_GET(self): 
    #global fRedirector, fCache
    #self.send_error(404)
    #print 'get'
    print 'recv: GET ' + self.path
    try:
      if 'utf-8' not in self.headers['Accept-Charset'].split(','):
        self.send_error(415)
        return
    except KeyError:
      #no this field
      pass
    '''
    try:
      if 'application/json' not in self.headers['Accept'].split(','):
        self.send_error(415)
        return
    except KeyError:
      pass
    '''
    pathComponents = self.path.split('/')[1:]
    #print pathComponents
    try:
      if pathComponents[0] == 'cache':
        if pathComponents[1] == 'names':
          self.getCacheNames()
          
        elif pathComponents[1] == 'resources':
          if len(pathComponents) > 3:
            res = '/'.join(pathComponents[3:])
            self.getCacheHitCountCurve(pathComponents[1], res)
          else:
            self.getCacheResources(pathComponents[2])
            
        elif pathComponents[1] == 'status':
          self.getCacheStatus(pathComponents[2])
        
        else:
          raise NameError
          
      elif pathComponents[0] == 'redirector':
        if pathComponents[1] == 'names':
          self.getRedirectorNames()
        elif pathComponents[1] == 'status':
          self.getRedirectorStatus(pathComponents[2])
        elif pathComponents[1] == 'redirectTable':
          self.getRedirectTable(pathComponents[2])
        elif pathComponents[1] == 'hitCountCurve':
          res = '/'.join(pathComponents[3:])
          self.getRedirectorHitCountCurve(pathComponents[2], res)
        else:
          raise NameError
        
      elif pathComponents[0] == 'controller':
        self.send_error(501)
        return
      elif pathComponents[0] == 'tests':
        if pathComponents[1] == 'pushRedirectTable':
          result = self.factoryRedirector.pushRedirectTable('redirector0', {'http://www.taobao.com/' : '192.168.100.32:80'})
          self.responseJson(result)
        elif pathComponents[1] == 'addRes':
          result = self.factoryCache.sendCachingCommand('cache0', 'http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png', 'addRes')
          self.responseJson(result)
        elif pathComponents[1] == 'removeRes':
          result = self.factoryCache.sendCachingCommand('cache0', 'http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png', 'removeRes')
          self.responseJson(result)
      elif pathComponents[0] == 'v1':
        if pathComponents[1]=='cache_info':
          caches = self.factoryCache.client_dict.keys()
          redirectors = self.factoryRedirector.client_dict.keys()
          
          cachecommand = [{'name':cache, 'call':'queryStatus'} for cache in caches] + \
                         [{'name':cache, 'call':'queryResources'} for cache in caches]
          
          redirectorcommand = [{'name' : redirector, 'call' : 'queryRedirectTable'} for redirector in redirectors]
          
          print "getting cache result"
          cacheresults = self.factoryCache.syncActiveGroupMessage(cachecommand)
          print "cache result get, getting redirector result"
          redirectorresults = self.factoryRedirector.syncActiveGroupMessage(redirectorcommand)
          print "redirector result get"
          #print cacheresults
          #print redirectorresults
          
          cache_nodes = {}
          redirector_nodes = {}
          
          for cache in caches:
            cache_nodes[cache] = {'ipcfg' : self.factoryCache.getClientAddress(cache)}
            
          for redirector in redirectors:
            redirector_nodes[redirector] = {'ipcfg' : self.factoryRedirector.getClientAddress(redirector)}
          
          for cacheresult in cacheresults:
            if cacheresult['call'] == 'queryStatus':
              cache_nodes[cacheresult['name']]['status'] = cacheresult['package']['result']
            elif cacheresult['call'] == 'queryResources':
              cache_nodes[cacheresult['name']]['resource'] = cacheresult['package']['result']
              
          for redirectorresult in redirectorresults:
            if redirectorresult['call'] == 'queryRedirectTable':
              redirector_nodes[redirectorresult['name']]['resources'] = redirectorresult['package']['result']
          
          dictroot = {
                      'errorcode':200, 'errorinfo':'OK', 
                      'cache_controller_id' : 'controller0',
                      'cache_nodes' : cache_nodes,
                      'redirector_nodes' : redirector_nodes
                      }
          
          self.responseJson(dictroot)
        elif pathComponents[1] == 'cache':
          if pathComponents[2][:5] == 'data=':
            data = pathComponents[2][5:]
            jsdata = json.loads(base64.b64decode(data))
            if jsdata.has_key('name') and jsdata.has_key('url'):
              self.getCacheHitCountCurve(jsdata['name'], jsdata['url'], True)
            else:
              self.responseJson({'errorcode':404, 'errorinfo':'Not found'})
 
          else:
            raise NameError
        elif pathComponents[1] == 'redirector':
          #self.responseJson(json.loads(data))
          if pathComponents[2][:5] == 'data=':
            data = pathComponents[2][5:]
            jsdata = json.loads(base64.b64decode(data))
            if jsdata.has_key('name') and jsdata.has_key('url'):
              self.getRedirectorHitCountCurve(jsdata['name'], jsdata['url'], True)
            else:
              self.responseJson({'errorcode':404, 'errorinfo':'Not found'})
          else:
            raise NameError
        else:
          raise NameError    
      else:
        raise NameError
    except (IndexError, NameError):
      self.send_error(404)
    except Exception, e:
      traceback.print_exc()
      self.send_error(500, str(e))

    '''      
    if self.path == '/caches':
      self.getCacheNames()
    elif self.path[0:len('/cache/')] == '/cache/':
      pathComponents = self.path[1:].split('/')
      if len(pathComponents) == 2:
        self.responseJson({'fields_avaliable' : ['status', 'resources'] })
      elif (len(pathComponents) == 3 and pathComponents[2] == 'status'):
        self.getCacheStatus(pathComponents[1])
      elif len(pathComponents) == 3 and pathComponents[2] == 'resources':
        self.getCacheResources(pathComponents[1])
      elif len(pathComponents) >= 4 and pathComponents[2] == 'resources':
        res = pathComponents[3]
        for frag in pathComponents[4:]:
          res = res + '/' + frag
        self.getCacheHitCountCurve(pathComponents[1], res)
      else:
        self.send_error(404)
        return
    elif self.path == '/redirectors':
      self.getRedirectorNames()
    elif self.path[0:len('/redirector/')] == '/redirector/':
      pathComponents = self.path[1:].split('/')
      if len(pathComponents) == 2:
        self.responseJson({'fields_avaliable' : ['status', 'redirectTable'] })
      elif (len(pathComponents) == 3 and pathComponents[2] == 'status'):
        self.getRedirectorStatus(pathComponents[1])
      elif len(pathComponents) == 3 and pathComponents[2] == 'redirectTable':
        self.getRedirectTable(pathComponents[1])
      elif len(pathComponents) >= 4 and pathComponents[2] == 'redirectTable':
        res = pathComponents[3]
        for frag in pathComponents[4:]:
          res = res + '/' + frag
        self.getRedirectorHitCountCurve(pathComponents[1], res)
      else:
        self.send_error(404)
        return
    elif self.path == '/controllers':
      pass
    #tests
    elif self.path == '/testpushredirecttable':
      result = self.factoryRedirector.pushRedirectTable('redirector0', {'http://www.taobao.com/' : '192.168.100.32:80'})
      self.responseJson(result)
    elif self.path == '/testaddres':
      result = self.factoryCache.sendCachingCommand('cache0', 'http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png', 'addRes')
      self.responseJson(result)
    elif self.path == '/testremoveres':
      result = self.factoryCache.sendCachingCommand('cache0', 'http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png', 'removeRes')
      self.responseJson(result)
      
    else:
      self.send_error(404)
    #self.writeCommonHeader('application/json')
    #self.wfile.write('hello world!')
    #buf = 'It works'
    #self.protocal_version = "HTTP/1.1"   
    #self.send_response(200)  
    #self.send_header("Welcome", "Content")
    #self.end_headers()  

    #self.wfile.write(buf)
  except Exception, e:
    self.send_error(500)
    print e
  '''
    
  def do_POST(self):
    print 'recv: POST ' + self.path
    try:
      postdata = self.rfile.read(int(self.headers['content-length']))
      print postdata
      post = json.loads(postdata)
      pathComponents = self.path.split('/')[1:]

      if pathComponents[0] == 'cache':

        if pathComponents[1] == 'resources':
          result = self.factoryCache.sendCachingCommand(pathComponents[2], post['url'], post['method'])
          self.responseJson(result)
          
            
        else:
          raise NameError
          
      elif pathComponents[0] == 'redirector':

        if pathComponents[1] == 'redirectTable':
          if post.has_key('flush'):
            flush = post['flush']
          else:
            flush = 1
            
          result = self.factoryRedirector.pushRedirectTable(pathComponents[2], post['table'], flush)
          self.responseJson(result)

        else:
          raise NameError
        
      elif pathComponents[0] == 'controller':
        self.send_error(501)
        return
      elif pathComponents[0] == 'tests':
        self.send_error(501)
      else:
        raise NameError
    except (IndexError, NameError):
      self.send_error(404)
    except (KeyError, ValueError):
      #json
      self.send_error(400)
    except Exception, e:
      print e
      self.send_error(500, str(e))

  
  def responseJson(self, jsondict):
    self.writeCommonHeader('application/json')
    self.wfile.write(json.dumps(jsondict))
  
  def getRedirectorNames(self):
    redirector = self.factoryRedirector.client_dict.keys()
    self.responseJson(redirector)

  def getRedirectorStatus(self, name):
    status = self.factoryRedirector.queryStatus(name)
    if status:
      self.responseJson(status)
    else:
      self.responseJson({'errorcode':404, 'errorinfo':'Not found'})
  
  def getRedirectTable(self, name):
    rdTable = self.factoryRedirector.queryRedirectTable(name)
    if rdTable:
      self.responseJson(rdTable)
    else:
      self.responseJson({'errorcode':404, 'errorinfo':'Not found'})
  
  def getRedirectorHitCountCurve(self, name, url, simpleRequest = False):
    hitcountcurve = self.factoryRedirector.queryHitCountCurve(name, url)
    if hitcountcurve:
      if simpleRequest:
        simpResponse = {'redirector':name, 'url': url, 
                        'errorcode':hitcountcurve['error'], 'errorinfo':hitcountcurve['errordescription']}
        if hitcountcurve['error']==200:
          simpResponse['hitcount'] = hitcountcurve['result']
        self.responseJson(simpResponse)
      else:
        self.responseJson(hitcountcurve)
    else:
      self.responseJson({'errorcode':404, 'errorinfo':'Not found'})

  def getCacheNames(self):
    caches = self.factoryCache.client_dict.keys()
    self.responseJson(caches)

  
  def getCacheStatus(self, name):
    status = self.factoryCache.queryStatus(name)
    if status:
      self.responseJson(status)
    else:
      self.responseJson({'errorcode':404, 'errorinfo':'Not found'})
      
  def getCacheResources(self, name):
    resources = self.factoryCache.queryResources(name)
    if resources:
      self.responseJson(resources)
    else:
      self.responseJson({'errorcode':404, 'errorinfo':'Not found'})

  
  def getCacheHitCountCurve(self, name, url, simpleRequest = False):
    hitcountcurve = self.factoryCache.queryHitCountCurve(name, url)
    if hitcountcurve:
      if simpleRequest:
        simpResponse = {'cache':name, 'url': url, 
                        'errorcode':hitcountcurve['error'], 'errorinfo':hitcountcurve['errordescription']}
        if hitcountcurve['error']==200:
          simpResponse['hitcount'] = hitcountcurve['result']
          
        self.responseJson(simpResponse)
      else:
        self.responseJson(hitcountcurve)
    else:
      self.responseJson({'errorcode':404, 'errorinfo':'Not found'})
      
  
  
  

def server_deamon(port):
  http_server = HTTPServer(('', int(port)), ControllerHTTPHandler)  
  print 'http server starting...'
  http_server.serve_forever()
  
def start_server(port, fr, fc):
  
  ControllerHTTPHandler.factoryCache = fc
  ControllerHTTPHandler.factoryRedirector = fr
  t = threading.Thread(target=server_deamon, args=(port,))
  t.setDaemon(True)
  t.start()

if __name__ == '__main__':
  
  pass