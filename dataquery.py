import numpy as np
import matplotlib.pyplot as pl

from rdmdate import dumpdate
from rdmdate import parsedate_myl as parsedate

def flattenoverlap(v,test=100,start=0):
  #Merge overlapping array of data. Expecting data in axis 1
  out=[v[0]]
  for j in range(1,len(v)):
    v1=v[j-1]
    v2=v[j]
    for i in range(start,len(v2)-test):
      s=sum(v1[-test:]-v2[i:i+test])
      if s==0:
        break
    newi=i+test
    out.append(v2[newi:])
    print newi
  return np.hstack(out)

from objdebug import ObjDebug

class DataQuery(ObjDebug,object):
  def __init__(self,source,names,t1,t2,data,**options):
    self.source=source
    self.names=names
    self.t1=t1
    self.t2=t2
    self.options=options
    self.data=data
  def set_source(self,source,**options):
    self.source=source
    self.options=options
    return self
  def reload(self,source=None,**options):
    dq=self.source.get(self.names,self.t1,self.t2,**self.options)
    self.data=dq.data
  def trim(self,before=None,after=None,relative=True,eps=1e-6):
    if after is not None:
      if relative is False:
        after=after-self.t2
      if after<0:
        self.t2+=after
        for name in self.names:
          idx,val=self.data[name]
          mask=idx<(self.t2)
          self.data[name]=idx[mask],val[mask]
      else:
        dq=self.source.get(self.names,self.t2,self.t2+after,**self.options)
        self.t2+=after
        for name in self.names:
          idx,val=self.data[name]
          nidx,nval=dq.data[name]
          np.concatenate([idx,nidx],axis=0)
          np.concatenate([val,nval],axis=0)
    if before is not None:
      if relative is False:
        before=self.t1-before
      if before<0:
        self.t1-=before
        for name in self.names:
          idx,val=self.data[name]
          mask=idx>(self.t1)
          self.data[name]=idx[mask],val[mask]
      else:
        dq=self.source.get(self.names,self.t1-before,self.t1-eps,
                           **self.options)
        self.t1-=before
        for name in self.names:
          idx,val=self.data[name]
          nidx,nval=dq.data[name]
          np.concatenate([nidx,idx],axis=0)
          np.concatenate([nval,val],axis=0)
  def add_sets(self,names):
    dq=self.source.get(names,self.t1,self.t2,**self.options)
    for name in names:
      self.data[name]=dq.data[name]
      self.names.append(name)
  def del_sets(self,names):
    for name in names:
      del self.data[name]
      self.names.remove(name)
  def __repr__(self):
    out=[]
    out.append("DataQuery %s"%str(self.source))
    out.append("  '%s' <--> '%s'" % (dumpdate(self.t1),dumpdate(self.t2)))
    for name in self.names:
      idx,val=self.data[name]
      typ="  ['%s'][1] => v%s"%(name,val.shape)
      out.append(typ)
    return '\n'.join(out)
  def store(self,source):
    for name in self.names:
      idx,val=self.data[name]
      source.store(name,idx,val)
  def flatten(self,name):
    idx,val=self.data[name]
    return flattenoverlap(val)
  def plot_specgramflat(self,name,NFFT=512,Fs=2,noverlap=0):
    t,val=self.data[name]
    val=flattenoverlap(val)
    pl.specgram(val,NFFT=NFFT,Fs=2,noverlap=noverlap)
    pl.title(name)
    return t,val








