import sqlite3

class IdxStore2(object):
  def __init__(self,filename,uid='INTEGER',idx='INTEGER', data='BLOB'):
    self.dbname=filename
    self.db=sqlite3.connect(self.dbname)
    c=self.db.cursor()
    tmp="""CREATE TABLE IF NOT EXISTS store
           (uid %s, idx %s, data %s)"""%(uid,idx,data)
    c.execute(tmp)
    tmp="""CREATE INDEX IF NOT EXISTS store_idx
           ON store (uid, idx)"""
    self.db.commit()
  def set(self,uid,idx,data):
    tmp="""INSERT INTO store VALUES (?,?,?)"""
    c=self.db.cursor()
    c.execute(tmp,(uid,idx,buffer(data)))
    self.db.commit()
  def get(self,uid=None,ida=None,idb=None):
    c=self.db.cursor()
    if idb is None:
      if ida is None:
        if uid is None:
          cmd=""
          arg=()
        else:
          cmd="WHERE uid=?"
          arg=(uid,)
      else:
        if uid is None:
          cmd="WHERE idx=?"
          arg=(ida,)
        else:
          cmd="WHERE uid=? AND idx=?"
          arg=(uid,ida)
    else:
      if uid is None:
        cmd="WHERE idx BETWEEN ? AND ?"
        arg=(ida,idb)
      else:
        cmd="WHERE uid=? AND idx BETWEEN ? AND ?"
        arg=(uid,ida,idb)
    c.execute('SELECT * FROM store %s  ORDER BY idx'%cmd,arg)
    for rec in c:
      yield(rec)
  def close(self):
    self.db.close()


if __name__=="__main__":
  db=IdxStore(":memory:")
  for i in xrange(30):
    db.set(1,i,"fasfd")
  for i in xrange(30):
    db.set(2,i,"fasfd")
  for uid,idx,data in db.get():
    print uid,idx,str(data)
  for uid,idx,data in db.get(2,10):
    print uid,idx,str(data)
  for uid,idx,data in db.get(2,11,13):
    print uid,idx,str(data)
  for uid,idx,data in db.get(None,11,13):
    print uid,idx,str(data)

