'''
Created on 2016-3-2

@author: hydra
'''
import re


class URLParser(object):
  '''
  Parse URL
  '''

  def __init__(self, url = None, port_default = 80):
    '''
    Constructor
    '''
    self.port_default = port_default
    self.url = url
    self.url_fields = None
    if url is not None:
      self.parse()
  
  def parse(self):
    if self.url is None:
      raise ValueError
    urlPattern = re.compile(r'((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?')
    match = urlPattern.match(self.url)
    if match is None:
      #print self.url
      raise ValueError
    matgroup = match.groups()
    host = matgroup[2]
    
    if matgroup[5] is None:
      port = self.port_default
    else:
      port = int(matgroup[5][1:])
      
    res = matgroup[6]
    self.url_fields = (host, port, res)
    return self.url_fields
  
      