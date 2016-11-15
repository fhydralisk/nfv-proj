import httplib,json
import base64

'''conn.request('GET', '/v1/redirector/data=' + base64.b64encode(json.dumps({'name':'redirector0', 'url':'http://hm.baidu.com:80/h.js?71d3a10afd40ef7a38a797fe02834a2f'})))
res = conn.getresponse()
print res.read()'''

def pushType():
  conn = httplib.HTTPConnection('10.192.0.13:8119')

  rdt  = {"00:00:96:6a:64:80:5f:44":"net.floodlightcontroller.recountfee.FeeTypeFlow"}
  jrdt = json.dumps(rdt)
  
  print jrdt
  
  conn.request('POST', '/charge/devices', body=jrdt)
  res = conn.getresponse()
  print res.read()
  conn.close()
  
  
def pushSubscriber():
  conn = httplib.HTTPConnection('10.192.0.13:8119')

  rdt  = {"192.168.233.3":1, "192.168.233.4":2, "192.168.233.6" : 8}
  jrdt = json.dumps(rdt)
  
  print jrdt
  
  conn.request('PUT', '/charge/subscribers', body=jrdt)
  res = conn.getresponse()
  print res.read()
  conn.close()


pushSubscriber()
#pushType()
