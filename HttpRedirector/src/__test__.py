import httplib,json
import base64

conn = httplib.HTTPConnection('10.192.0.13:8118')
'''conn.request('GET', '/v1/redirector/data=' + base64.b64encode(json.dumps({'name':'redirector0', 'url':'http://hm.baidu.com:80/h.js?71d3a10afd40ef7a38a797fe02834a2f'})))
res = conn.getresponse()
print res.read()'''

rdt  = {"table":{"http://www.ndiy.cn/data/attachment/forum/201603/14/083756goxeedj8dx9p87f7.jpg" : "172.17.0.4:80"}, "flush" : 0}

conn.request('POST', '/redirector/redirectTable/redirector0@1f30f5508d01', body=json.dumps(rdt))
res = conn.getresponse()
print res.read()

'http://www.ndiy.cn/data/attachment/forum/201603/14/083756goxeedj8dx9p87f7.jpg'
#conn.request('POST', '/redirector/redirectTable/redirector0', json.dumps({'table':{'http://www.youku.com/' : '192.168.100.26:80'}}))
#res = conn.getresponse()
#print res.read()

conn.close()

'''
result = self.factoryRedirector.pushRedirectTable('redirector0', {'http://www.youku.com/' : '192.168.100.26:80'})
          self.responseJson(result)
'''
          