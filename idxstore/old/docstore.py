import sqlite3
import cPickle as Pickle


class doc(object):
  def __init__(self,content,tags=[],roles=[],parents=[],
               docid=None,timestamp=None):
    self.content=content




class DocStore(object):
  def __init__(self,filename):
    self.dbname=filename
    self.db=sqlite3.connect(self.dbname)
    c=self.db.cursor()
    cmd="""
    CREATE TABLE IF NOT EXISTS docs
       (docid INTEGER, timestamp INTEGER, content TEXT,
        tags TEXT, parents TEXT, roles TEXT);
    CREATE TABLE IF NOT EXISTS users
       (userid INTEGER, name TEXT, secret TEXT, roles TEXT);
    CREATE TABLE IF NOT EXISTS roles
       (id INTEGER, name TEXT, perms INTEGER);
    CREATE TABLE IF NOT EXISTS alias
       (id INTEGER, name TEXT);
    CREATE TABLE IF NOT EXISTS tags
       (id INTEGER, name TEXT);
    """
  def get_doc(self,docid,timestamp=None,last=True):
    if timestamp is None:
      cmd="SELECT * FROM  docs WHERE docid = ? ORDER BY timestamp DESC"
      c.execute(cmd,(docid,))
      if last==True:
        return self._extract_doc(c.next())
      else:
        for rec in c:
          yield self._extract_doc(rec)
    else:
      cmd="SELECT * FROM  docs WHERE docid = ? AND timestamp = ?"
      c.execute(cmd,(docid,timestamp))
      return self._extract_doc(c.next())
  def post_doc(self,docid,content,tags,parents,roles,timestamp=None):
    if timestamp is None:
      timestamp=int(time.time()*1e6)
    cmd="INSERT INTO store VALUES (?,?,?,?,?,?)"
    content=Pickle.dumps(content)
    tags=[ self._get_id_by_name('tags',t) for t in tags]
    roles=[ self._get_id_by_name('roles',t) for t in roles]
    c.execute(cmd,(docid,timestamp,content,tags,parents,roles))
    c.commit()
    return doc
  def put_doc(self,docid,content,tags,roles,timestamp=None):
    oldrec=self.get_doc(docid)
    parents=[(docid,rec[1])]
    self.post_doc(docid,content,tags,parents,roles,timestamp)
  def post_user(self,name,secret,roles):
    pass
  def post_role(self,name,perms)
    pass
  def post_tag(self,name)
    pass
  def get_tags(self,name)
    pass
  def _extract_doc(self,rec):
    docid,timestamp,content,tags,parents,roles = rec
    content=Pickle.loads(content)
    tags=[ self._get_name_by_id('tags',t) for t in tags]
    roles=[ self._get_name_by_id('roles',t) for t in roles]
    return docid,timestamp,content,tags,parents,roles
  def _get_id_by_name(self,kind,name):
    cmd="SELECT id FROM %s WHERE name = ?"%(kind)
    return c.execute(cmd,(name,)).next()
  def _get_name_by_id(self,kind,uid):
    cmd="SELECT name FROM %s WHERE id = ?"%(kind)
    return c.execute(cmd,(name,)).next()









