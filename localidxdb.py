import os
from glob import glob
import re

from idxstore import IdxStore
from rdmdate import dumpdate
from localdate import parsedate,dumpdate,SearchName

from dataquery import DataQuery

#import idxstore
#reload(idxstore)
#IdxStore=idxstore.IdxStore

class LookUp(object):
  pass

def mkkeyword(s):
  s=s.replace('.','_').replace(':','_')
  return "".join(x for x in s if x.isalpha() or x.isdigit() or x=='_')

#from objdebug import ObjDebug as object
class LocalIdxDB(SearchName,object):
  def get_names(self):
    return self.tables.keys()
  def __init__(self,location):
    self.dirname=location
    self.reload()
  def reload(self):
    indexfn=IdxStore._indexname
    self.tables={}
    self.lookup=LookUp()
    print os.path.join(self.dirname,'*',indexfn)
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
    names=self._parsenames(names)
    data={}
    for name in names:
      db=self.tables[name]
      data[name]=db.get(t1,t2)
    dq=DataQuery(self,names,t1,t2,data)
    return dq
  def store(self,name,idx,val):
    if name in self.tables:
      db=self.tables[name]
    else:
      db=IdxStore(os.path.join(self.dirname,mkkeyword(name)))
      db.name=name
      self.tables[name]=db
      setattr(self.lookup,mkkeyword(name),name)
    db.store(idx,val)
  def rebalance(self,tablename,maxpagesize):
    db=self.tables[tablename]
    db.maxpagesize=maxpagesize
    db.rebalance()
  def prune(self,tablename):
    db=self.tables[tablename]
    db.prune()





