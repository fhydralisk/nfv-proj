'''
Created on 

@author: hydra
'''
#not used
import socket, threading
from src.lib import HTTPRequest
from src.redirector import RedirectTable



class ProxyClient(object):
  '''
  classdocs
  '''
  client_addr = None
  client_socket = None
  client_remoteport = 0
  
  client_thread = None
  

  def __init__(self, addr, controllerPort, sock, recv_timeout = 20, free_timeout = 1800):
    '''
    Constructor
    '''
    
    if addr is None or sock is None:
      return
    
    self.client_addr = addr
    self.client_remoteport = controllerPort
    self.client_socket = sock
    sock.settimeout(recv_timeout)
    
    self.client_thread = threading.Thread(target=self.client_deamon, args=self)
    self.client_thread.setDaemon(True)
    self.client_thread.start()
    
    
    
  def client_deamon(self):
    peer = RedirectTable.RedirectTable()
    client_prevouspeer = None
    peer_socket = None
    
    while 1:
      try:
        data = self.client_socket.recv(1024)
        request = HTTPRequest.HTTPRequest(data)
        if request.error_code <> 0:
          continue
        res = request.path
        host = request.headers['host']
        print host
        peernode = peer.getPeerNode(host, res, self.client_remoteport)
        
        if client_prevouspeer == peernode and peer_socket is not None:
          peer_socket.send(data)
        else:
          if peer_socket is not None:
            peer_socket.close()
          
          peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          peer_socket.settimeout(self.conto)
          try:
            peer_socket.connect(peernode)
            peer_socket.send(data)
          except socket.timeout:
            #should 404 here
            continue
        
        peer_socket.settimeout(self.revto) 
        while 1:
          try:
            response = peer_socket.recv(1024)
          except socket.timeout:
            break
          except:
            peer_socket.close()
            peer_socket = None
            break
          
          self.client_socket.send(response)

        
        
      except socket.timeout:
        pass
      
      except:
        print "client_deamon error"
        self.client_socket.close()
        if peer_socket is not None:
          peer_socket.close()
        return
    pass
      