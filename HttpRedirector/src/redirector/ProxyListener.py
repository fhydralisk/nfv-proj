'''
Created on 

@author: hydra
'''

import socket, threading
from src.redirector import ProxyClient

class ProxyListener(object):
  '''
  classdocs
  '''
  controllerPort = 0
  listen_thread = None
  listen_socket = None
  client_chain = None

  def __init__(self, client_chain, controllerPort = 80, controllerIP = '', timeout = None):
    '''
    Constructor
    '''
    
    self.client_chain = client_chain
    
    try:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.bind((controllerIP, controllerPort))
      s.settimeout(timeout)
      #s.listen(2)
      
    except:
      print 'listen socket create failed'
      s.close()
      return 
    
    self.port = controllerPort
    self.listen_socket = s
    
  def start(self):
    if (self.listen_socket is not None and self.listen_thread is None):
      self.listen_thread = threading.Thread(target=self.listening_thread, args=self)
      self.listen_thread.setDaemon(True)
      self.listen_thread.start()
      
    
  def listening_thread(self):
    self.listen_socket.listen(1)
    while True:
      try:
        conn_socket, addr = self.listen_socket.accept()
        pclient = ProxyClient.ProxyClient(sock=conn_socket, addr=addr, controllerPort=self.port)
        self.client_chain.append(pclient)
      except socket.timeout:
        pass
      except:
        self.listen_socket.close()
        return
      
    pass
    
        