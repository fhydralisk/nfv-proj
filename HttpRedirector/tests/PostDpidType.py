'''
Created on 2016-3-20

@author: hydra
'''

import commands, time, httplib, StringIO, json, sys

fakeovsctl = '''975dcabe-2aa8-4749-889e-ad8fec781b86
    Bridge "br0"
        Controller "tcp:10.192.0.13:6653"
            is_connected: true
        Port "br0"
            Interface "br0"
                type: internal
        Port "eth0"
            Interface "eth0"'''

fakedpid = '"0000660ab5391949"'

def ovsctl():
  (outval, output) = commands.getstatusoutput("ovs-vsctl show")
  return output

def get_dpid():
  (outval, output) = commands.getstatusoutput("ovs-vsctl get bridge br0 datapath-id")
  return output

def parseOvsCtl(text):
  parser = StringIO.StringIO(text)
  lines = parser.readlines()
  fields = {'connected' : False, 'controller ip' : '10.192.0.13', 'controller port' : 6653 }
  
  for line in lines:
    l = line.strip()
    if l.startswith('Controller'):
      tcpf = l.split('"')[1]
      ipf = tcpf.split(":")
      ip = ipf[1]
      try:
        port = int(ipf[2])
      except IndexError:
        port = 6633
      
      fields['controller ip'] = ip
      fields['controller port'] = port
    elif l.startswith('is_connected'):
      conn = bool(l.strip(" ")[1])
      fields['connected'] = conn
      
  return fields

def parseDpid(text):
  text0 = text.strip().strip('"')
  result = ""
  for i in range(len(text0)):
    result = result + text0[i]
    if i % 2 == 1:
      result = result + ':'
  
  return result.rstrip(':')

def run(port, ctype):
  
  confimer = 0
  while (confimer < 3):
    if (parseOvsCtl(ovsctl())['connected']):
      confimer += 1
    else:
      confimer = 0
    
    time.sleep(3)
    
  #connected
  ovsinfo = parseOvsCtl(ovsctl())
  controllerIp = ovsinfo['controller ip']
  dpid = parseDpid(get_dpid())
  
  print commands.getoutput('ovs-ofctl add-flow br0 "in_port=1, priority=220, ip, nw_src=%s, actions:output=local"' % controllerIp)
  print commands.getoutput('ovs-ofctl add-flow br0 "in_port=local, priority=220, ip, nw_dst=%s, actions:output=1"' % controllerIp)
  
  conn = httplib.HTTPConnection(controllerIp, port)
  
  post = {dpid : ctype}
  conn.request('POST', "/charge/devices", json.dumps(post))
  print conn.getresponse().read()
  

run(int(sys.argv[1]), sys.argv[2])
