'''
Created on 2016-3-2

@author: hydra
'''

import os, json
from controller.ControllerRunner import ControllerRunner
from controller import HttpServer


if __name__ == '__main__':
  
  try:
    f=open(os.path.expanduser('~/controller.conf'),'r')
  except Exception, e:
    print "~/controller.conf not found."
    print e
    exit()
    
  try:
    profile = json.load(f)
  except Exception, e:
    print "bad configuration file."
    print e
    exit()
  finally:
    f.close()
  
  if not profile:
    print "empty or bad configuration file. check controller.conf"
    exit()
    
  try:
    restport = profile['port_server_rest'] 
    redirectorPort = profile['port_server_redirector']
    cachePort = profile['port_server_cache']
    #cachePaylaodLearn = profile['cache_curve_count_to_learn_in_min']
    
    #coordinatorRestUri = profile['uri_client_coordinator_rest']
    coordinatorRestAddr = profile['addr_client_coordinator']
    coordinatorRestPort = profile['port_client_coordinator']
    #profile['username_client_coordinator_rest']
    #profile['password_client_coordinator_rest']
    
    
  except KeyError:
    print "bad configuration file. check controller.conf" 
  
  
  controllerrunner = ControllerRunner(redirector_port=redirectorPort, cache_port=cachePort, interface='')
  controllerrunner.profile = profile
  #print controllerrunner.controlServer.cacheFactory
  HttpServer.start_server(restport, controllerrunner.controlServer.redirectorFactory, controllerrunner.controlServer.cacheFactory)
  controllerrunner.run_server()

