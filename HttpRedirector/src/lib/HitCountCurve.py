'''
Created on 2016

@author: hydra
'''

import time

class HitCountCurve(object):
  '''
  A curve object holds url's hit count
  '''
  
  '''
  curve : [
    {timestamp : ts,
    count : count}, ...
  ]
  '''
  
  
  def __init__(self, time_granularity = 30, time_hold = 5, timestamp_ref = 0, loadCurve = None):
    '''
    @param time_granularity: granularity of hit count curve in table, in minutes
    @param time_hold:  how much times in granularity the table holds, in granularity
    '''
    self.time_granularity = time_granularity
    self.time_hold = time_hold
    self.timestamp_ref = timestamp_ref
    if loadCurve and isinstance(loadCurve, list):
      self.curve = loadCurve
    else:
      self.curve = []
    
    pass
  
  def hitNow(self, count = 1):
    self.hitAtTimeStamp(time.time(), count)
    
    
  def hitAtTimeStamp(self, ts, count = 1):
    
    ts = int(ts)
    
    granularity_in_sec = 60 * self.time_granularity
    
    #get time group. for e.g. 19:35 and 19:45 would be in 19:30 group and 20:01 would be in 20:00 group
    #in timestamp
    timegroup = ts - (ts % granularity_in_sec)
    
    #cleanup hit-count before time_hold
    while (len(self.curve)>0) and \
          (self.curve[0]['timestamp'] < timegroup - 60 * self.time_granularity * self.time_hold):
      del self.curve[0]
    
    if len(self.curve) > 0 and self.curve[len(self.curve) - 1]['timestamp'] > timegroup:
      raise Exception('TimeError', 'Time Not Linear.')
    
    if len(self.curve) == 0 or self.curve[len(self.curve) - 1]['timestamp'] < timegroup:
      #append 0 hit curve before this hit first.
      try:
        timegroup_last = self.curve[len(self.curve) - 1]['timestamp']
      except IndexError:
        timegroup_last = timegroup - 60 * self.time_granularity * self.time_hold
      
      timegroup_firstappend = timegroup_last + 60 * self.time_granularity
      for t in range(timegroup_firstappend, timegroup, 60 * self.time_granularity):
        self.curve.append({'timestamp': t, 'count' : 0})
      self.curve.append({'timestamp' : timegroup, 'count' : 0})
    
    self.curve[len(self.curve) - 1]['count'] = self.curve[len(self.curve) - 1]['count'] + count
    
    pass
        
  
  def getTable(self):
    return self.curve
  
  def toJsonDict(self):
    return {'curve' : self.curve, 'time_options' : (self.time_granularity, self.time_hold, self.timestamp_ref)}
  
  def __str__(self, *args, **kwargs):
    return self.__class__.__name__ + ':' + str(self.curve)
  
  @classmethod
  def fromJsonDict(cls, jsondict):
    hitCountObj = cls()
    hitCountObj.time_granularity = jsondict['time_options'][0]
    hitCountObj.time_hold = jsondict['time_options'][1]
    hitCountObj.timestamp_ref = jsondict['time_options'][2]
    hitCountObj.curve = jsondict['curve']
    return hitCountObj
  
  
  