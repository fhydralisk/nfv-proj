'''
Created on 2016-3-20

@author: hydra
'''

import time,commands, httplib, sys, re

def useHttplib(url):
  '''
  conn = httplib.HTTPConnection('10.192.0.13:8118')
  conn.request('GET', url)
  res = conn.getresponse()
  '''
  urlPattern = re.compile(r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?')
  match = urlPattern.match(url)
  if match is None:
    #print self.url
    raise ValueError
  matgroup = match.groups()
  host = matgroup[2]
  
  if matgroup[5] is None:
    port = None
  else:
    port = int(matgroup[5][1:])
    
  res = matgroup[6]
  
  if port:
    host = host + ":" + str(port)
  conn = httplib.HTTPConnection(host)
  conn.request('GET', res)
  res= conn.getresponse()

  #print res.read()


def useSquieclient(redirector, url):
  commands.getoutput("squidclient -h %s -p 80 %s" % (redirector, url))

def run(url, redirector = None):
  timestart = time.time()
  
  if redirector is None:
    useHttplib(url)
  else:
    useSquieclient(redirector, url)
    
  timeend = time.time()
  
  
  print "Time: %s s" % str(timestart-timeend)
  


if len(sys.argv) == 2:
  run(sys.argv[1])
elif len(sys.argv) == 3:
  run(sys.argv[2], sys.argv[1])
else:
  print "usage: TestHttpGet <url>"
  print "       TestHttpGet <redirector> <url>"