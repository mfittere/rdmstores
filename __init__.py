from localidxdb import LocalIdxDB
from cernlogdb2 import CernLogDB
from localdate import parsedate,dumpdate,tounixtime,dumpdateutc
from dataquery import DataQuery,flattenoverlap, set_xaxis_date, set_xlim_date, get_xlim_date

class Autoload(object):
  def __init__(self,cls,*args,**argn):
    self.cls=cls
    self.args=args
    self.argn=argn
  def __getattribute__(self,k):
    ga=object.__getattribute__
    sa=object.__setattr__
    cls=ga(self,'cls')
    args=ga(self,'args')
    argn=ga(self,'argn')
    sargs=','.join(map(repr,args))
    sargn=','.join(['%s=%s'%(k,v) for k,v in argn.items()])
    print "Loading %s(%s)"%(cls.__name__,','.join([sargs,sargn]))
    dic=cls(*args,**argn).__dict__
    sa(self,'__class__',cls)
    sa(self,'__dict__',dic)
    return ga(self,k)


lcldb =Autoload(LocalIdxDB,"/home/rdemaria/data/lhcfilldata/datadb")
logdb =CernLogDB(datasource='LHCLOG_PRO_DEFAULT')
measdb=CernLogDB(datasource='MEASDB_PRO_DEFAULT')


#def rdmstore():
#  print "Reading lcldb, logdb, measdb"
#  lcldb=LocalIdxDB("/home/rdemaria/pcbe13028/datastore")
#  logdb=CernLogDB(datasource='lhclog_pro_default')
#  measdb=CernLogDB(datasource='MEASDB_PRO_DEFAULT')
#
