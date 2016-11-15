'''
Created on 2016-3-2

@author: hydra
'''
import sys,os,json, socket
from src.redirector.RedirectorRunner import RedirectorRunner
from src.status.CacheRunner import CacheRunner

def printHelp():
  print 'syntax: python HttpRedirector <name> <device> <controllerIP> <controllerPort>'
  print '        python HttpRedirector for rd.conf in home dir with jsondict inside'
  print json.dumps ({'name':'devicename', 'dev':'devicetype', 'controllerIP':'ip of controller', 'controllerPort' : 8118}, indent=4)
  exit()

if __name__ == '__main__':
    
    if len(sys.argv) >= 5:
      name = sys.argv[1]
      dev = sys.argv[2]
      controllerIP = sys.argv[3]
      controllerPort = int(sys.argv[4])
      
      if dev == 'cache':
        pathLog = None
        if len(sys.argv) == 6:
          pathLog = sys.argv[5]

    elif len(sys.argv) == 1:
      try:
        fp = open(os.path.expanduser('~/rd.conf'))
      except IOError:
        printHelp()
      
      cmd = json.load(fp)
      fp.close()
      
      try:
        name = cmd['name']
        dev = cmd['dev']
        controllerIP = cmd['controllerIP']
        controllerPort = cmd['controllerPort']
        if dev == 'cache':
          pathLog = None
          if cmd.has_key('pathlog'):
            pathLog = cmd['pathlog']
      except KeyError:
        printHelp()
      
    else:
      printHelp()
      
      
    name = name + '@' + socket.gethostname()
    
    if dev == 'redirector':
      rr = RedirectorRunner(name, (controllerIP, controllerPort), None)
      rr.runNode()
      rr.runServer()
      rr.run()
      
    elif dev == 'cache':
      cr = CacheRunner(name, (controllerIP, controllerPort), None, pathLog)
      cr.runNode()
      cr.run()
      
    else:
      printHelp()