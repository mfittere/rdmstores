from localidxdb import LocalIdxDB
from cernlogdb2 import CernLogDB

lcldb=LocalIdxDB("/home/rdemaria/pcbe13028/datastore")
logdb=CernLogDB(datasource='LHCLOG_PRO_DEFAULT')
measdb=CernLogDB(datasource='MEASDB_PRO_DEFAULT')
