import os
from glob import glob
import re

from idxstore import IdxStore
from rdmdate import dumpdate
from rdmdate import parsedate_myl as parsedate

class LookUp(object):
  pass

def mkkeyword(s):
  return "".join(x for x in s if x.isalpha() or x.isdigit() or x=='_')

from objdebug import ObjDebug

class LocalIdxDB(ObjDebug,object):
  def __init__(self,location):
    self.dirname=location
    self.reload()
  def search(self,regexp):
    r=re.compile(regexp,re.IGNORECASE)
    return r.findall('\n'.join(self.tables.keys()))
  def _parsenames(self,names):
    if not hasattr(names,'__iter__'):
      out=[]
      for name in names.split(','):
        if name.startswith('/'):
          out.extend(self.search(name[1:]))
        else:
          out.append(name)
    return out
  def reload(self):
    indexfn=IdxStore._indexname
    self.tables={}
    self.lookup=LookUp()
    for i in glob(os.path.join(self.dirname,'*',indexfn)):
      dirname,indexname=os.path.split(i)
      table=IdxStore(dirname)
      self.tables[table.name]=table
      setattr(self.lookup,mkkeyword(table.name),table)
  def get(self,names,t1,t2):
    """Query local database are return QueryData
    names:  name od tables, comma separated or list
    t1,t2:  start and end time as string in the %Y-%m-%d %H:%M:%S.SSS format
            or unix time
    nmax    max number of records
    """
    t1=parsedate(t1)
    t2=parsedate(t2)
    #t1=rdts(mkts(t1),tz=False,millisec=True,tsep='T')
    #t2=rdts(mkts(t2),tz=False,millisec=True,tsep='T')
    if hasattr(names,'__iter__'):
      names=','.join(names)
  def store(self,name,idx,val):
    if name in self.tables:
      db=self.tables[name]
    else:
      db=IdxStore(os.path.join(self.dirname,mkkeyword(name)),name=name)
      self.tables[name]=db
      setattr(self.lookup,mkkeyword(name),name)
    db.store(idx,val)




