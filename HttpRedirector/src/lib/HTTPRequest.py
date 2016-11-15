'''
Created on
@author: hydra
'''

from BaseHTTPServer import BaseHTTPRequestHandler
from StringIO import StringIO
 
class HTTPRequest(BaseHTTPRequestHandler):
  def __init__(self, request_text):
    self.rfile = StringIO(request_text)
    self.raw_requestline = self.rfile.readline()
    self.error_code = self.error_message = None
    self.parse_request()
  
  def send_error(self, code, message):
    self.error_code = code
    self.error_message = message
  
  def string_by_adding_proxy_header(self, controllerPort):
    if self.rfile is None:
      raise Exception('ValueError','rfile empty')
    
    rawdata = self.rfile.getvalue()
    return rawdata.replace(self.command + ' /', self.command + ' http://' + self.headers['host'] + ':' + str(controllerPort) + '/');
  
  def string_by_removing_proxy_header(self, controllerPort):
    if self.rfile is None:
      raise Exception('ValueError','rfile empty')
    
    rawdata = self.rfile.getvalue()
    return rawdata.replace(self.command + ' http://' + self.headers['host'], self.command + ' ');
    