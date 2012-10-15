import time
import re


from rdmdate import parsedate_myl

def parsedate(t):
  try:
    if type(t) is complex:
      t=time.time()-t.imag
    float(t)
    return t
  except ValueError:
    return parsedate_myl(t)

def dumpdate(t,fmt='%Y-%m-%d %H:%M:%S.SSS'):
  ti=int(t)
  tf=t-ti
  s=time.strftime(fmt,time.localtime(t))
  if 'SSS' in s:
    s=s.replace('SSS','%03d'%(tf*1000))
  return s


#from objdebug import ObjDebug as object

class SearchName(object):
  def search(self,regexp):
    r=re.compile(regexp,re.IGNORECASE)
    res=[ l for l in self.get_names() if r.search(l) is not None]
    return res
  __floordiv__=search
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

