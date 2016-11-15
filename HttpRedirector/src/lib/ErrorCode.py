'''
Created on 2016-3-2

@author: hydra
'''

class ErrorCode(object):
  error200_ok = (200, 'OK')
  error404_notfound = (404, 'Not Found')
  error400_badrequest = (400, 'Bad Request')
  error500_internalerror = (500, 'Internal error')
  error501_notimplemented = (501, 'Not implemented')