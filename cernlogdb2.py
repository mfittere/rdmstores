import re
import os
import time
import cPickle

import numpy as np

from cernlogdb import dbget
from dataquery import DataQuery

from localdate import parsedate,dumpdate,SearchName

dirname=os.path.dirname(os.path.abspath(__file__))
varlistfn=os.path.join(dirname,'cernlogdb_varlist.pky')
varlist=cPickle.load(open(varlistfn))


#def readvarlist(base='/home/rdemaria/work/rdmstores/cerndbnames'):
#  out=[]
#  for fn in os.listdir(base):
#    if fn.isalpha():
#      ffn=os.path.join(base,fn)
#      if os.path.isfile(ffn):
#        out.extend([i.strip() for i in open(ffn).readlines()])
#      elif os.path.isdir(ffn):
#        out.extend(readvarlist(ffn))
#  out=list(set(out))
#  return out


exe_path=os.path.dirname(os.path.abspath(__file__))
exe_path=os.path.join(exe_path,'cern-logdb3')


#from objdebug import ObjDebug as object
class CernLogDB(SearchName,object):
  conf_template="""\
CLIENT_NAME=%s
APPLICATION_NAME=%s
DATASOURCE_PREFERENCES=%s
TIMEZONE=%s
FILE_DIRECTORY=%s
UNIX_TIME_OUTPUT=%s"""
  def get_names(self):
    return varlist
  def __repr__(self):
    return "CernLogDB('%s')"%self.datasource
  def __init__(self,
               datasource='LHCLOG_PRO_DEFAULT',
               client_name='BEAM_PHYSICS',
               app_name='LHC_MD_ABP_ANALYSIS',
               timezone='LOCAL_TIME',
               file_dir='./',
               unix_time_output='TRUE',
               exe_path=exe_path,
               conffn='./ldb.conf'):
    self.client_name=client_name
    self.app_name=app_name
    self.datasource=datasource
    self.timezone=timezone
    self.file_dir=file_dir
    self.unix_time_output=unix_time_output
    self.exe_path=exe_path
  def set_datasource(self,datasource):
    """Set data source in
       LHCLOG_PRO_DEFAULT, LHCLOG_TEST_DEFAULT,
       MEASDB_PRO_DEFAULT, MEASDB_DEV_DEFAULT,
       LHCLOG_PRO_ONLY, LHCLOG_TEST_ONLY, MEASDB_PRO_ONLY"""
    self.datasource=datasource
  def get(self,names,t1=None,t2=None,step=None,scale=None,debug=False,types=(float,float),method='DS',verbose=True):
    """Query the CERN measurement database and return QueryData
    names:  name of the variables in the database: comma separated or list
    t1,t2:  start and end time as string in the %Y-%m-%d %H:%M:%S.SSS format
            or unix time
    step:   For multiple file request '<n> <size>'
    scale:  For scaling algoritm '<n> <size> <alg>'
    types:  type to convert timestamp and data. If None, no concatenation is performed

    where:
      <n> is an integer number
      <size> is one of SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
      <alg> one of AVG, MIN, MAX, REPEAT, INTERPOLATE, SUM, COUNT
    """
    if t2 is None:
      t2=time.time()
    if t1 is None:
      t1=t2-1
      print t1,t2
      method='LD'
    t1=dumpdate(parsedate(t1))
    t2=dumpdate(parsedate(t2))
    names=self._parsenames(names)
    if verbose:
      print "CernLogDB: querying\n  %s"%'\n  '.join(names)
      print "CernLogDB: '%s' <--> '%s'"% (t1,t2)
      print "CernLogDB: options %s %s"% (step,scale)
    res=dbget(names,t1,t2,step=step,scale=scale,
               exe=self.exe_path,conf=None,
               client_name=self.client_name,
               app_name=self.app_name,
               datasource=self.datasource,
               timezone=self.timezone,
               method=method,
               types=types)
    log='\n'.join(res['log'])
    if debug:
      print log
    if method=='LD':
      res=parse_ld(log,types)
    data={}
    bad=[]
    for k in names:
      if k in res:
        data[k]=res[k]
      else:
        bad.append(k)
    if len(bad)>0:
      print log
      raise IOError, "CernLogDB %s not retrieved" %','.join(bad)
    dq=DataQuery(self,names,parsedate(t1),parsedate(t2),data,step=step,scale=scale)
    if method=='LD':
      dq.trim()
    return dq
  def lhc_fillnumber(self,t1=None,t2=None):
    if t2 is None:
      t2=time.time()
    if t1 is None:
      t1=t2-365*24*3600
    data=self.get('HX:FILLN',t1,t2)
    duration=np.diff(np.r_[data.a0,t2])
    fmt="%6d '%s' %8.2f h"
    for fillnu,beg,duration in zip(data.a1,data.a0,duration):
      dumpdate(beg)
      print fmt%(fillnu,dumpdate(beg),duration/3600.)
#  def lhc_intensity_attiming(self,timing,delta=60,
#      int1='LHC.BCTFR.A6R4.B1:BEAM_INTENSITY',
#      int2='LHC.BCTFR.A6R4.B2:BEAM_INTENSITY'):
#    pass






def parse_ld(s,types=(float,float)):
  data={}
  ttype,vtype=types
  for line in s.split('\n'):
    if line.startswith('Variable:'):
      name=line.strip().split('Variable: ')[1]
      #t,v=np.zeros(1,dtype=float),np.zeros(1,dtype=float)
      t,v=[],[]
      data[name]=[t,v]
    elif 'Timestamp' in line and 'Value' in line:
      no,ts,val=line.split(': ')
      ts=parsedate(ts.split('"')[1])
      val=val.strip()
      if val.startswith('{'):
        val=val[1:-1].split(',')
      t.append(ts)
      v.append(val)
  for name,(t,v) in data.items():
    t=np.array(t,dtype=ttype)
    v=np.array(v,dtype=vtype)
    data[name]=[t,v]
  return data





