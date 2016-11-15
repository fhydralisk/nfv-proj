'''
Created on 2016-2-25

@author: hydra
'''

from SquidLogAnal import SquidLogAnal
from SquidStateAnal import SquidStateAnalyzer

def run():
  print SquidLogAnal().analyze()
  print SquidStateAnalyzer().analyze()
  
  
if __name__ == '__main__':
  run()
