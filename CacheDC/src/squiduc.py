'''
Created on 2016-1-12

@author: hydra
'''


import socket, sys

#host = sys.argv[1]
#textport = sys.argv[2]

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('127.0.0.1', 60000))
while 1:
  print "Enter data to transmit:"
  data = sys.stdin.readline().strip()
  s.sendall(data)
