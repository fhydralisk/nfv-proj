'''
Created on 2016-2-25

@author: hydra
'''

import commands
from StringIO import StringIO

'''
HTTP/1.1 200 OK
Server: squid/3.3.8
Mime-Version: 1.0
Date: Sun, 28 Feb 2016 11:10:49 GMT
Content-Type: text/plain
Expires: Sun, 28 Feb 2016 11:10:49 GMT
Last-Modified: Sun, 28 Feb 2016 11:10:49 GMT
X-Cache: MISS from mininet-desk
X-Cache-Lookup: MISS from mininet-desk:80
Via: 1.1 mininet-desk (squid/3.3.8)
Connection: close

Squid Object Cache: Version 3.3.8
Start Time:  Thu, 25 Feb 2016 06:42:41 GMT
Current Time:  Sun, 28 Feb 2016 11:10:49 GMT
Connection information for squid:
  Number of clients accessing cache:  2
  Number of HTTP requests received:  40
  Number of ICP messages received:  0
  Number of ICP messages sent:  0
  Number of queued ICP replies:  0
  Number of HTCP messages received:  0
  Number of HTCP messages sent:  0
  Request failure ratio:   0.00
  Average HTTP requests per minute since start:  0.0
  Average ICP messages per minute since start:  0.0
  Select loop called: 1835356 times, 149.992 ms avg
Cache information for squid:
  Hits as % of all requests:  5min: 0.0%, 60min: 0.0%
  Hits as % of bytes sent:  5min: 100.0%, 60min: 100.0%
  Memory hits as % of hit requests:  5min: 0.0%, 60min: 0.0%
  Disk hits as % of hit requests:  5min: 0.0%, 60min: 0.0%
  Storage Swap size:  96196 KB
  Storage Swap capacity:   0.5% used, 99.5% free
  Storage Mem size:  232 KB
  Storage Mem capacity:   0.0% used, 100.0% free
  Mean Object Size:  43.12 KB
  Requests given to unlinkd:  4
Median Service Times (seconds)  5 min    60 min:
  HTTP Requests (All):   0.00000  0.00000
  Cache Misses:          0.00000  0.00000
  Cache Hits:            0.00000  0.00000
  Near Hits:             0.00000  0.00000
  Not-Modified Replies:  0.00000  0.00000
  DNS Lookups:           0.00000  0.00000
  ICP Queries:           0.00000  0.00000
Resource usage for squid:
  UP Time:  275287.834 seconds
  CPU Time:  2.918 seconds
  CPU Usage:  0.00%
  CPU Usage, 5 minute avg:  0.00%
  CPU Usage, 60 minute avg:  0.00%
  Process Data Segment Size via sbrk(): 6664 KB
  Maximum Resident Size: 72752 KB
  Page faults with physical i/o: 12
Memory usage for squid via mallinfo():
  Total space in arena:    6796 KB
  Ordinary blocks:         6703 KB     44 blks
  Small blocks:               0 KB      0 blks
  Holding blocks:         10968 KB      7 blks
  Free Small blocks:          0 KB
  Free Ordinary blocks:      93 KB
  Total in use:              93 KB 1%
  Total free:                93 KB 1%
  Total size:             17764 KB
Memory accounted for:
  Total accounted:         1320 KB   7%
  memPool accounted:       1320 KB   7%
  memPool unaccounted:    16444 KB  93%
  memPoolAlloc calls:     61241
  memPoolFree calls:      61492
File descriptor usage for squid:
  Maximum number of file descriptors:   16384
  Largest file desc currently in use:     14
  Number of file desc currently in use:   14
  Files queued for open:                   0
  Available number of file descriptors: 16370
  Reserved number of file descriptors:   100
  Store Disk files open:                   0
Internal Data Structures:
    2284 StoreEntries
      55 StoreEntries with MemObjects
      54 Hot Object Cache Items
    2231 on-disk objects
'''

class squidStateParser(object):
  '''
  parse squid state
  '''
  
  
  
  def __init__(self, statetext):
    self.state_text = statetext
    self.state = {}
    self.state_structure = {}
    self.parse()
  
  def parse(self):
    sio = StringIO(self.state_text)
    l=sio.readline()
    
    while 1:
      
      if l=='':
        break
      
      if l[0]=='\t':
        print "indent error"
      
      field = l.strip()
      self.state_structure[field] = []
      
      while 1:
        lin = sio.readline()
        if lin=='' or lin[0] <> '\t':
          l = lin
          break

        self.state_structure[field].append(lin.strip())
    
    
    self.parseCPU(self.state_structure['Resource usage for squid:'])
    self.parseStorage(self.state_structure['Cache information for squid:'])
    self.parseRequests(self.state_structure['Connection information for squid:'])
    
    
    
  def parseCPU(self, lines):
    for l in lines:
      if l.startswith('CPU'):
        p = l.split(':')
        self.state[p[0].strip()] = p[1].strip()
        
    pass
  
  def parseStorage(self, lines):
    for l in lines:
      if l.startswith('Storage'):
        p = l.split(':')
        self.state[p[0].strip()] = p[1].strip()

    pass
  
  def parseRequests(self, lines):
    for l in lines:
      if l.startswith('Number'):
        p = l.split(':')
        self.state[p[0].strip()] = int(p[1].strip())

  
  
class squidRequestsParser(object):
  
  def __init__(self, statetext):
    self.state_text = statetext
    self.state = {}
    self.parse()
    
  def parse(self):
    lines = self.state_text.split('\n')
    #print lines
    for line in lines:
      if line.startswith('client_http.requests'):
        self.state = {'client_http.requests' : float(line.replace('client_http.requests = ', '').split('/')[0])}
        break
  

class SquidStateAnalyzer(object):
  
  def __init__(self):
    pass
  
  def analyze(self):
    try:
      (rval, outval) = commands.getstatusoutput('squidclient -h 127.0.0.1 mgr:info')
      state1 = squidStateParser(outval).state
      (rval, outval) = commands.getstatusoutput('squidclient -h 127.0.0.1 mgr:5min')
      state2 = squidRequestsParser(outval).state
      state = dict(state1, **state2)
    except KeyError:
      state = {}
      
    return state
    