import operator
import os

import numpy as np

class RawData(object):
  @classmethod
  def get_allpages(cls,basedir):
    out=[]
    for fn in os.listdir(basedir):
      if fn.endswith('_val.dat'):
        out.append(int(fn.split('_')[0]))
    return out
  def remove(self,basedir):
    os.unlink(self.get_val_fn(basedir))
    os.unlink(self.get_idx_fn(basedir))
  def __init__(self,uid,idxtype,idxshape,valtype,valshape):
    self.uid=uid
    self.idxtype=np.dtype(idxtype)
    self.idxshape=idxshape
    self.valtype=np.dtype(valtype)
    self.valshape=valshape
    self.valsize=self.valtype.itemsize*reduce(operator.mul,valshape)
  def get_val_fn(self,basedir):
    return os.path.join(basedir,"%d_val.dat"%self.uid)
  def get_idx_fn(self,basedir):
    return os.path.join(basedir,"%d_idx.dat"%self.uid)
  def load_idx(self,basedir,skip=None,count=-1):
    fn=self.get_idx_fn(basedir)
    arr=np.fromfile(fn,dtype=self.idxtype,count=count)
    arr=arr.reshape(self.idxshape)
    if skip is not None:
      arr=arr[::skip]
    return arr
  def load_val(self,basedir,skip=None,count=-1):
    fn=self.get_val_fn(basedir)
    arr=np.fromfile(fn,dtype=self.valtype,count=count)
    arr=arr.reshape(self.valshape)
    if skip is not None:
      arr=arr[::skip]
    return arr
  def load(self,basedir,skip=None):
    idx=self.load_idx(basedir,skip=skip)
    val=self.load_val(basedir,skip=skip)
    return idx,val
  def store(self,idx,val,basedir):
    idx.tofile(self.get_idx_fn(basedir))
    val.tofile(self.get_val_fn(basedir))
  def get_range(self):
    idx=self.load_idx()
    return idx[0],idx[-1]
  def toJson(self):
    out=['RawData',self.uid,str(self.idxtype),self.idxshape,
        str(self.valtype),self.valshape]
    return out
  def can_cat_with(self,other):
    if self.valshape[1:]!=other.valshape[1:]:
      return False
    if self.valtype!=other.valtype:
      return False
    if self.idxtype!=other.idxtype:
      return False
    if self.idxshape!=other.idxshape:
      return False
    return True


