'''
Created on 2016-3-2

@author: hydra
'''


from ManageServer import ControlServer

class ControllerRunner(object):
  
  def __init__(self, redirectorPort, cachePort, interface=''):
    '''
    @param controllerAddr:(ip, port) of controller
    @param bindAddr: (ip, port) to bind to connec to controller 
    @param serverPort: port to run as a redirector server
    @param serverInterface: ip of interface to bind to listen
    '''
    self.redirectorPort= redirectorPort
    self.cachePort= cachePort
    self.interface = interface
    self.controlServer =  ControlServer(self.interface, (self.cachePort, self.redirectorPort))
    
    self.profile = None
    
  def runServer(self):
   
    self.controlServer.profile = self.profile
    self.controlServer.run()
    

