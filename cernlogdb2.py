import re

from cernlogdb import dbget
from dataquery import DataQuery
from rdmdate import parsedate_myl as parsedate



_varnames="""\
LHC.BQBBQ.CONTINUOUS.B1:FFT_DATA_H
LHC.BQBBQ.CONTINUOUS.B1:FFT_DATA_V
LHC.BQBBQ.CONTINUOUS.B2:FFT_DATA_H
LHC.BQBBQ.CONTINUOUS.B2:FFT_DATA_V
LHC.BQBBQ.CONTINUOUS.B1:ACQ_DATA_H
LHC.BQBBQ.CONTINUOUS.B1:ACQ_DATA_V
LHC.BQBBQ.CONTINUOUS.B2:ACQ_DATA_H
LHC.BQBBQ.CONTINUOUS.B2:ACQ_DATA_V
"""

from objdebug import ObjDebug


class CernLogDB(ObjDebug,object):
  conf_template="""\
CLIENT_NAME=%s
APPLICATION_NAME=%s
DATASOURCE_PREFERENCES=%s
TIMEZONE=%s
FILE_DIRECTORY=%s
UNIX_TIME_OUTPUT=%s"""
  def search(self,regexp):
    r=re.compile(regexp,re.IGNORECASE)
    return r.findall(_varnames)
  def __repr__(self):
    return "CernLogDB('%s')"%self.datasource
  def __init__(self,
               datasource='LHCLOG_PRO_DEFAULT',
               client_name='BEAM_PHYSICS',
               app_name='LHC_MD_ABP_ANALYSIS',
               timezone='LOCAL_TIME',
               file_dir='./',
               unix_time_output='TRUE',
               exe_path='cern-logdb',
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
  def get(self,names,t1,t2,step=None,scale=None):
    """Query the CERN measurement database and return QueryData
    names:  name of the variables in the database: comma separated or list
    t1,t2:  start and end time as string in the %Y-%m-%d %H:%M:%S.SSS format
            or unix time
    step:   For multiple file request '<n> <size>'
    scale:  For scaling algoritm '<n> <size> <alg>'

    where:
      <n> is an integer number
      <size> is one of SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
      <alg> one of AVG, MIN, MAX, REPEAT, INTERPOLATE, SUM, COUNT
    """
    t1=parsedate(t1)
    t2=parsedate(t2)
    if not hasattr(names,'__iter__'):
      out=[]
      for name in names.split(','):
        if name.startswith('/'):
          out.extend(self.search(name[1:]))
        else:
          out.append(name)
      names=out
    res=dbget(names,t1,t2,step=step,scale=scale,
               exe=self.exe_path,conf=None,
               client_name=self.client_name,
               app_name=self.app_name,
               datasource=self.datasource,
               timezone=self.timezone)
    data=dict( [ (k,res[k]) for k in names ])
    dq=DataQuery(self,names,t1,t2,data,step=step,scale=scale)
    return dq






