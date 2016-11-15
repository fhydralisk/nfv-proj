'''
Created on 2016-1-12

@author: hydra
'''

import socket, commands

host = ''
port = 60000

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((host, port))

while 1:
  try:
    message, address = s.recvfrom(8192)
    print message
    if message[:5] == 'CACHE':
      #cache command
      (retval, outval) = commands.getstatusoutput('squidclient -m GET ' + message[5:])
      print outval
      pass
    elif message[:6] == 'DELETE':
      #purge command
      (retval, outval) = commands.getstatusoutput('squidclient -m PURGE ' + message[6:])
      print outval
      pass
    
  except (KeyboardInterrupt, SystemExit):
    raise
  except:
    pass

