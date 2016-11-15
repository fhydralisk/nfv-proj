'''
Created on 2016年2月29日

@author: hydra
'''
import sys, os

sys.path.append(object)



from status.SquidLogAnal import SquidLogAnal
from status.SquidStateAnal import SquidStateAnalyzer

if __name__ == '__main__':
  hits =  SquidLogAnal().analyze()
  for k in hits:
    print (k, str(hits[k]))
  print SquidStateAnalyzer().analyze()
