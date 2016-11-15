'''
Created on 2016-3-2

@author: stackoverflow
'''

import httplib
from StringIO import StringIO

http_response_str = """HTTP/1.1 200 OK
Date: Thu, Jul  3 15:27:54 2014
Content-Type: text/xml; charset="utf-8"
Connection: close
Content-Length: 626"""

class FakeSocket():
  def __init__(self, response_str):
    self._file = StringIO(response_str)
  def makefile(self, *args, **kwargs):
    return self._file


class HTTPResponse(object):
  def __init__(self, response_str):
    source = FakeSocket(response_str)
    self.response = httplib.HTTPResponse(source)
    self.response.begin()
'''
print "status:", response.status
print "single header:", response.getheader('Content-Type')
print "content:", response.read(len(http_response_str)) # the len here will give a 'big enough' value to read the whole content
'''