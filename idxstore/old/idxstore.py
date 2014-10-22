import os
import cPickle as pickle
import sqlite3
import  gzip
from zlib import compress,decompress

def dumps(obj):
  return buffer(pickle.dumps(obj))

def loads(s):
  return pickle.loads(str(s))


def dumpz(obj):
  return buffer(compress(pickle.dumps(obj)))

def loadz(s):
  return pickle.loads(decompress(str(s)))

class IdxStore(object):
  """key object store using sqlite3
    where key is an integer and
    object is any pickleable python object"""
  def __init__(self,dbname):
    self.dbname=dbname+'.db'
    if not os.path.exists(self.dbname):
      self.db=sqlite3.connect(self.dbname)
      c=self.db.cursor()
      tmp="""CREATE TABLE IF NOT EXISTS store
             (idx INTEGER, data BLOB)"""
      c.execute(tmp)
      tmp="""CREATE INDEX IF NOT EXISTS store_idx
             ON store (idx)"""
      c.execute(tmp)
  def count(self,idx):
    c=self.db.cursor()
    cmd='SELECT count(*) FROM store WHERE idx=?'
    c.execute(cmd,(idx,))
    return c.next()[0]
  def first(self):
    c=self.db.cursor()
    cmd='SELECT MIN(idx) FROM store'
    c.execute(cmd)
    return c.next()[0]
  def last(self):
    c=self.db.cursor()
    cmd='SELECT MAX(idx) FROM store'
    c.execute(cmd)
    return c.next()[0]
  def __len__(self):
    c=self.db.cursor()
    cmd='SELECT count(*) FROM store'
    c.execute(cmd)
    return c.next()[0]
  def __contains__(self,idx):
    return self.count(idx)!=0
  def set(self,idx,data):
    """set item
    remeber to close or commit to store the data
    """
    c=self.db.cursor()
    tmp="""INSERT INTO store VALUES (?,?)"""
    c.execute(tmp,(idx,dumps(data)))
  def get(self,ida=None,idb=None):
    """return a data from the index
    if two index are provided the data in range is returned"""
    c=self.db.cursor()
    if ida is None:
      cmd=""
      arg=()
    else:
      if idb is None:
        cmd="WHERE idx=?"
        arg=(ida,)
      else:
        cmd="WHERE idx BETWEEN ? AND ?"
        arg=(ida,idb)
    c.execute('SELECT * FROM store %s  ORDER BY idx'%cmd,arg)
    for idx,data in c:
      yield( (idx,loads(data)) )
  def prune_duplicates(self):
    c=self.db.cursor()
    tmp="""DELETE FROM store WHERE EXISTS (
       SELECT rowid from store s2 WHERE s2.idx=store.idx AND
       s2.rowid < store.rowid)"""
    c.execute(tmp)
    self.db.commit()
  def sync(self):
    self.db.commit()
  def close(self):
    self.db.commit()
    self.db.close()


class IdxStoreGz(IdxStore):
  """key object store using sqlite3
    where key is an integer and
    object is any pickleable python object"""
  def set(self,idx,data):
    """set item
    remeber to close or commit to store the data
    """
    c=self.db.cursor()
    tmp="""INSERT INTO store VALUES (?,?)"""
    c.execute(tmp,(idx,dumpz(data)))
  def get(self,ida=None,idb=None):
    """return a data from the index
    if two index are provided the data in range is returned"""
    c=self.db.cursor()
    if ida is None:
      cmd=""
      arg=()
    else:
      if idb is None:
        cmd="WHERE idx=?"
        arg=(ida,)
      else:
        cmd="WHERE idx BETWEEN ? AND ?"
        arg=(ida,idb)
    c.execute('SELECT * FROM store %s  ORDER BY idx'%cmd,arg)
    for idx,data in c:
      yield( (idx,loadz(data)) )


class IdxStoreExt(IdxStore):
  """key object store using sqlite3
    where key is an integer and
    object is any pickleable python object"""
  def __init__(self,dbname):
    if not os.path.isdir(dbname):
      os.mkdir(dbname)
    self.dirname=dbname
    self.dbname=dbname+'.db'
    self.db=sqlite3.connect(self.dbname)
    c=self.db.cursor()
    tmp="""CREATE TABLE IF NOT EXISTS store
           (idx INTEGER)"""
    c.execute(tmp)
  def set(self,idx,data):
    """set item
    remeber to close or commit to store the data
    """
    self._dump(data,idx)
    c=self.db.cursor()
    tmp="""INSERT INTO store VALUES (?)"""
    c.execute(tmp,(idx,))
  def get(self,ida=None,idb=None):
    """return a data from the index
    if two index are provided the data in range is returned"""
    c=self.db.cursor()
    if ida is None:
      cmd=""
      arg=()
    else:
      if idb is None:
        cmd="WHERE idx=?"
        arg=(ida,)
      else:
        cmd="WHERE idx BETWEEN ? AND ?"
        arg=(ida,idb)
    c.execute('SELECT * FROM store %s  ORDER BY idx'%cmd,arg)
    for idx in c:
      data=self._load(idx)
      yield( (idx,data) )
  def _load(self,idx):
    n='%020i'%idx
    l=[n[0:4],n[4:8],n[8:12],n[12:16],n[16:20]]
    fname=os.path.join(self.dirname,*l)
    return pickle.load(file(fname))
  def _dump(self,data,idx):
    n='%020i'%idx
    l=[n[0:4],n[4:8],n[8:12],n[12:16]]
    dirname=os.path.join(self.dirname,*l)
    if not os.path.isdir(dirname):
      os.makedirs(dirname)
    fname=os.path.join(dirname,n[16:20])
    pickle.dump(data,file(fname,'w'))


class IdxStoreExtGz(IdxStoreExt):
  """key object store using sqlite3
    where key is an integer and
    object is any pickleable python object"""
  def _load(self,idx):
    n='%020i.gz'%idx
    l=[n[0:4],n[4:8],n[8:12],n[12:16],n[16:]]
    fname=os.path.join(self.dirname,*l)
    return pickle.load(gzip.open(fname))
  def _dump(self,data,idx):
    n='%020i.gz'%idx
    l=[n[0:4],n[4:8],n[8:12],n[12:16]]
    dirname=os.path.join(self.dirname,*l)
    if not os.path.isdir(dirname):
      os.makedirs(dirname)
    fname=os.path.join(dirname,n[16:])
    pickle.dump(data,gzip.open(fname,'w'))


if __name__=="__main__":
  db=IdxStore(":memory:")
  from numpy import arange,frombuffer
  for i in xrange(30):
    db.set(i,arange(i))
  for idx,data in db.get():
    print idx,repr(data)
  for idx,data in db.get(10):
    print idx,repr(data)
  for idx,data in db.get(11,13):
    print idx,frombuffer(data,dtype=int)

