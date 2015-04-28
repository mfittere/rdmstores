"""
Wrapper around the  CERN logging database command line tool

Usage:
  mkcmd:  Produce a string for the CERN logging database command line tool
  getdb:  Query and parse the results
  open:   Parse and return the output of a query from a file name
  load:   Parse and return the output of a query from a file object
  pprint: Describe the output of getdb, open, load
"""


import os
import time
import gzip

import numpy as _np

import rdmdate as _date

import time as _t


exe_path=os.path.dirname(os.path.abspath(__file__))
exe_path=os.path.join(exe_path,'cern-logdb3')

#print exe_path

conf_template="""\
CLIENT_NAME=%s
APPLICATION_NAME=%s
DATASOURCE_PREFERENCES=%s
TIMEZONE=%s
FILE_DIRECTORY=%s
UNIX_TIME_OUTPUT=%s"""

def dbget(vs,t1,t2=None,step=None,scale=None,filename='dummy',format='CSV',exe=exe_path,conf=None,
          client_name='BEAM_PHYSICS',
          app_name='LHC_MD_ABP_ANALYSIS',
          datasource='LHCLOG_PRO_DEFAULT',
          timezone='LOCAL_TIME',
          file_dir='./',
          unix_time_output='TRUE',
          method='DS',
          types=(float,float),debug=False):
  """Query the CERN measurement database and return data
  Usage dbget("SPS.BCTDC.31832:INT","2010-06-10 00:00:00","2010-06-10 23:59:59",step='20 SECOND')

  Arguments:
    vs:          name of the variables in the database: comma separated or list
    t1,t2:       start and end time as string in the %Y-%m-%d %H:%M:%S.SSS format
                 or unix time
    step:        For multiple file request '<n> <size>'
    scale:       For scaling algoritm '<n> <size> <alg>'
    format:      For output file format in CSV, XLS, TSV, MATHEMATICA
    filename:    String for output name
    conf:        Configuration file name, if None, file is created from options
    client_name: The name of the client (must be defined together with the BE/CO/DM section)
    app_name:    The name of the application (must be defined together with the BE/CO/DM section)
    datasource:  Where the data should be extracted '<dbname>'
    timezone:    The time zone used for input and output data
    file_dir:    Defines the folder where output files will be written
    unix_time_output: Timestamp are written in seconds for epoch 'TRUE' or FALSE

  where:
    <n> is an integer number
    <size> is one of SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
    <alg> one of AVG, MIN, MAX, REPEAT, INTERPOLATE, SUM, COUNT
    <dbname> one of LHCLOG_PRO_DEFAULT, LHCLOG_TEST_DEFAULT, MEASDB_PRO_DEFAULT, MEASDB_DEV_DEFAULT,
                    LHCLOG_PRO_ONLY, LHCLOG_TEST_ONLY, MEASDB_PRO_ONLY
    <tz> one of 'UTC_TIME' 'LOCAL_TIME'
  """
#  cmd=mkcmd(vs,t1,t2,step,scale,exe,'dummy','CSV',method,
#             conf,client_name,app_name,datasource,timezone,file_dir,unix_time_output)
  cmd=mkcmd(vs,t1,t2,step,scale,exe,filename,format,method,
             conf,client_name,app_name,datasource,timezone,file_dir,unix_time_output)
  os.system(cmd)
  fh=file(os.path.join(file_dir,filename+'.'+format))
  data=load(fh,types=types)
  fh.close()
  if debug==False:
    os.unlink(os.path.join(file_dir,filename+'.'+format))
  return data

def mkcmd(vs,t1,t2,step=None,scale=None,
          exe=exe_path,filename='output.csv',format='CSV',method='DS',
          conf=None,
          client_name='BEAM_PHYSICS',
          app_name='LHC_MD_ABP_ANALYSIS',
          datasource='LHCLOG_PRO_DEFAULT',
          timezone='LOCAL_TIME',
          file_dir='./',
          unix_time_output='TRUE'):
  """Produce a string for the CERN logging database command line tool.

  Usage getdata("SPS.BCTDC.31832:INT","2010-06-10 00:00:00","2010-06-10 23:59:59",step='20 SECOND')

  Arguments:
    vs:      name of the variable in the database
    t1,t2:  start and end time as string in the %Y-%m-%d %H:%M:%S.SSS format
            or unix time
    step:   For multiple file request '<n> <size>'
    scale:  For scaling algoritm '<n> <size> <alg>'
    format: For output file format in CSV, XLS, TSV, MATHEMATICA
    filename: String for output name
    method: DS (dataset), LD (last data), FD (fill data not supported here)
    conf: Configuration file name, if None, file is created from options
    client_name:      The name of the client (must be defined together with the BE/CO/DM section)
    app_name: The name of the application (must be defined together with the BE/CO/DM section)
    datasource: Where the data should be extracted '<dbname>'
    timezone: The time zone used for input and output data
    file_dir: Defines the folder where output files will be written
    unix_time_output: Timestamp are written in seconds for epoch 'TRUE' or FALSE

  where:
    <n> is an integer number
    <size> is one of SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
    <alg> one of AVG, MIN, MAX, REPEAT, INTERPOLATE, SUM, COUNT
    <dbname> one of LHCLOG_PRO_DEFAULT, LHCLOG_TEST_DEFAULT, MEASDB_PRO_DEFAULT, MEASDB_DEV_DEFAULT,
                    LHCLOG_PRO_ONLY, LHCLOG_TEST_ONLY, MEASDB_PRO_ONLY
    <tz> one of 'UTC_TIME' 'LOCAL_TIME'
  """
  if not conf:
    conf='ldb.conf'
    fh=file(conf,'w')
    fh.write(conf_template%(client_name,app_name,datasource,timezone,file_dir,unix_time_output))
  if type(t1) in [float,int]:
    if t1<0:
      t1+=_t.time()
    t1=_date.dumpdate(t1)
  if type(t2) in [float,int]:
    if t2<0:
      t2+=_t.time()
    t2=_date.dumpdate(t2)
  if hasattr(vs,'__iter__'):
    vs=','.join(vs)
  cmd='%s -vs "%s" -t1 "%s" -C %s' %(exe,vs,t1,conf)
  if t2:
    cmd+=' -t2 "%s"' % t2
  if scale:
    n,size,alg=scale.split()
    if alg in ['AVG', 'MIN', 'MAX', 'REPEAT', 'INTERPOLATE', 'SUM', 'COUNT']:
      cmd+=' -sa "%s" -ss "%s" -si "%s"' %(alg,n,size)
    else:
      raise ValueError,'cernlogdb: %s alg not supported'%alg
  if step:
    ni,it=step.split()
    if it in ['SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR']:
      cmd+=' -IT "%s" -NI "%s"' %(it,ni)
    else:
      raise ValueError,'cernlogdb: %s step not supported'%it
  if filename:
    cmd+=' -N "%s"' % filename
  if format:
    cmd+=' -F "%s"' % format
  if method:
    cmd+=' -M "%s"' % method
  return cmd

_interval={'day': 86400,
       'hour': 3600,
       'minute': 60,
       'month': 2592000,
       'second': 1,
       'week': 604800,
       'year': 31536000}




def generate_filenames(mask,t1,t2,step):
  """ Generate filenames as done by the CERN DB command line tool

  Arguments:
    mask string
    step:   For multiple file request '<n> <size>'
    t1,t2:  start and end time as string in the %Y-%m-%d %H:%M:%S.SSS format
            or unix time

  where:
    <n> is an integer number
    <size> is one of SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR
  """
  if type(t1) in [float,int]:
    t1=_date.dumpdate(t1)
  if type(t2) in [float,int]:
    t2=_date.dumpdate(t2)
  ni,it=step.split()
  sstep=int(ni)*_interval[it]



def load(fh,sep=',',t1=None,t2=None,debug=False,nmax=1e99,types=(float,float)):
  """Parse the output of the CERN logging database command line tool

  Usage:
    fh:    file handler
    sep:   separator ',' or '\\t'
    t1,t2: time interval read
    nmax:  maximum number of records per variable

  Returns:
    A dictionary for which for each variable found data is stored in a tuple
    of timestamps list and record list. Data is accesible by the variable name
    and a numeric index.
    In addition the keys:
      'log'      contains the log messages contained in the file
      'datavars' contains the a list of variable names
  """
  if type(t1) is str:
    t1=_date.parsedate(t1)
  if type(t2) is str:
    t2=_date.parsedate(t2)
  data={}
  dataon=False
  header=True
  log=[]
  datavars=[]
  for l in fh:
    if l.startswith('VARIABLE'):
      vname=l.split()[1]
      count=0
      if debug is True:
        print 'Found var %s' % vname
      if vname in data:
        t,v=data[vname]
      else:
        t,v=[],[]
        data[vname]=[t,v]
        datavars.append(vname)
      dataon=False
      header=False
    elif l.startswith('Timestamp'):
      dataon=True
      tformat='string'
      if 'UNIX Format' in l:
        tformat='unix'
      elif 'LOCAL_TIME' in l:
        tformat='local'
      elif 'UTC_TIME' in l:
        tformat='utc'
    elif l=='\n':
      dataon=False
    elif dataon:
      ll=l.strip().split(sep)
      if tformat=='unix':
        trec=float(ll[0])/1000.
      elif tformat=='utc':
        trec=_date.parsedate_myl(ll[0]+' UTC')
      else:
        trec=_date.parsedate_myl(ll[0])
      #print trec,t1,t2,
      #print (t1 is None or trec>=t1) and (t2 is None or trec<=t2)
      if (t1 is None or trec>=t1) and (t2 is None or trec<=t2) and count<nmax:
        t.append(trec)
        vrec=ll[1:]
        v.append(vrec)
        count+=1
    elif header:
      if debug is True:
        print l
      log.append(l)
  if len(log)>0:
    data['log']=log
  data['datavars']=datavars
  for ii,nn in enumerate(datavars):
    data[ii]=data[nn]
  if types is not None:
    ttype,vtype=types
    data=combine_data(data,vtype=vtype,ttype=ttype)
  return data

def combine_data(data,vtype=float,ttype=float):
  """Combine and change data type"""
  for ik,k in enumerate(data['datavars']):
    t,v=data[k]
    t=_np.array(t,dtype=ttype)
    v=_np.array(v,dtype=vtype)
    if v.shape[-1]==1:
      v=v.reshape(v.shape[0])
    data[k]=[t,v]
    data[ik]=data[k]
  return data

def open(fn,sep=None,t1=None,t2=None,nmax=1e99,debug=False):
  """Load output of the CERN measurement database query from a filename

  Usage:  open("test.tsv")
    fn: filename
    sep: separator type

  Separator is inferred from the file extension as well
  """
  if sep is None:
    if fn.endswith('tsv') or fn.endswith('tsv.gz'):
      sep='\t'
    else:
      sep=','
  if fn.endswith('.gz'):
    fh=gzip.open(fn)
  else:
    fh=file(fn)
  data=load(fh,sep=sep,debug=debug,t1=t1,t2=t2,nmax=nmax)
  fh.close()
  data['filename']=fn
  return data


def openfnames(fnames,sep=None,t1=None,t2=None,nmax=1e99,debug=False):
  """Open a set of filenames"""
  mask=fnames[0]
  if sep is None:
    if mask.endswith('tsv') or mask.endswith('tsv.gz'):
      sep='\t'
    else:
      sep=','
  data=load(_icat(fnames),sep=sep,debug=debug,t1=t1,t2=t2,nmax=nmax)
  data['filename']=fnames
  return data

def _icat(fnames):
  for fn in fnames:
    if fn.endswith('.gz'):
      for l in gzip.open(fn):
        yield l
    else:
      for l in file(fn):
        yield l


def pprint(data):
  """Pretty print data, last dimension is from the first record"""
  print "CERN DB data:"
  for k in data['datavars']:
    t,v=data[k]
    recl=set()
    for ii,vv in enumerate(v):
      recl.add(len(vv))
    recl=' or '.join([str(i) for i in recl])
    print "  ['%s'][1] => v[%d][%s]" %( k,len(t),recl)


def dbget_repeat(vs,t1,t2,step,sa=None,sf=None,exe='cern-ldb'):
  if type(t1) is str:
    t1=round(_date.parsedate_myl(t1),0)
  if type(t2) is str:
    t2=round(_date.parsedate_myl(t2),0)
  data_final={}
  data_final['log']=[]
  data_final['datavars']=vs.split(',')
  for vname in data_final['datavars']:
    data_final[vname]=([],[])
  for t in range(int(t1),int(t2),step):
    nt1=_date.dumpdate(t)
    nt2=_date.dumpdate(t+step-0.001)
    print 'calling dbget from %s to %s' % (nt1,nt2)
    data=dbget(vs,nt1,nt2,sa=sa,sf=sf,exe=exe)
    for vname in data['datavars']:
      t,v=data[vname]
      nt=data_final[vname][0].extend(t)
      nv=data_final[vname][0].extend(v)
    data_final['log'].extend(data['log'])
  return data_final

def merge_out(fnames):
  data_final={}
  data_final['log']=[]
  data_final['datavars']=vs.split(',')
  for vname in data_final['datavars']:
    data_final[vname]=([],[])
  for fn in fnames:
    data=load(fn)
    for vname in data['datavars']:
      t,v=data[vname]
      nt=data_final[vname][0].extend(t)
      nv=data_final[vname][1].extend(v)
    data_final['log'].extend(data['log'])
  return data_final


def interpolate(data,tnew):
  datanew=data.copy()
  for vi,vn in enumerate(data['datavars']):
    t,v=data[vn]
    vnew=_np.interp(tnew,t,v)
    datanew[vn]=tnew,vnew
    datanew[vi]=datanew[vn]
  return datanew



