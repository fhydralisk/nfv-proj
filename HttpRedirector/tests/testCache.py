'''
Created on 2016-3-18

@author: hydra
'''


import commands, sys, time

reslist = ['http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png',
           'http://www.ndiy.cn/data/attachment/forum/201603/14/083756goxeedj8dx9p87f7.jpg']


def run(redirector, counts, reslist):
  '''
  for i in range(count1):
    print "geting resource1 " + str(i)
    commands.getstatusoutput('squidclient -h %s -p 80 http://www.ndiy.cn/template/huyouxiong_sqhd/src/logo.png'%redirector)
  for i in range(count2):
    print "geting resource2 " + str(i)
    commands.getstatusoutput('squidclient -h %s -p 80 http://www.ndiy.cn/data/attachment/forum/201603/14/083756goxeedj8dx9p87f7.jpg'%redirector)
  '''
  
  while True:
    getcount = min(len(reslist), len(counts))
    timestart = time.time()
    
    for i in range(getcount):
      for j in range(counts[i]):
        commands.getstatusoutput('squidclient -h %s -p 80 %s' % (redirector, reslist[i]) )
        print "Get %s via %s %s times" % (reslist[i], redirector, str(j))
    
    time2sleep = 5 - ( time.time() - timestart )
    if time2sleep <= 0:
      time2sleep = 0.5
    
    time.sleep(time2sleep)
    pass
    
    
    
redirectorip = sys.argv[1]
counts = [int(n) for n in sys.argv[2:]]

run(redirectorip, counts, reslist)