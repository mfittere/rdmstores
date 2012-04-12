import time
import re

from rdmdate import parsedate_myl

def parsedate(t):
  if type(t) in [int,float]:
    return t
  else:
    return parsedate_myl(t)

def dumpdate(t,fmt='%Y-%m-%d %H:%M:%S.SSS'):
  ti=int(t)
  tf=t-ti
  s=time.strftime(fmt,time.localtime(t))
  if 'SSS' in s:
    s=s.replace('SSS','%03d'%round(tf*1000))
  return s


from objdebug import ObjDebug

class SearchName(ObjDebug,object):
  def search(self,regexp):
    r=re.compile(regexp,re.IGNORECASE)
    res=[ l for l in self.names if r.search(l) is not None]
    return res
  def _parsenames(self,names):
    if not hasattr(names,'__iter__'):
      out=[]
      for name in names.split(','):
        if name.startswith('/'):
          res=self.search(name[1:])
          print "Using the following names:"
          for name in res:
            print name
          out.extend(res)
        else:
          out.append(name)
      names=out
    return names

