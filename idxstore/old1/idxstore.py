"""
IdxStore Riccardo De Maria
Array database optimized for ver big dataset and range queries
Support:
  get: range query
  store: range load
  remove: range remove
  rebalance: change page size

Structure:
  %d_idx.npy index in npy format
  %d_val.npy value in npy format
  000indexj  Json index
    ibeg,iend,nrec,idxtype,idxshape,valtype,valshape,name

#KNOWN BUGS
#test with dense data sometime fails

#TODO
# get history
# prune history
# test different record types
# review locking mechanism
# abstract index object
# protect setting internal attributes
## remove json dependece
## implement btree index
"""
import os
import shutil
import time
from operator import itemgetter
from glob import glob

import numpy as np
import simplejson

class Page(tuple):
  __slots__=()
  ibeg     =property(itemgetter(0))
  iend     =property(itemgetter(1))
  nrec     =property(itemgetter(2))
  idxtype  =property(itemgetter(3))
  idxshape =property(itemgetter(4))
  valtype  =property(itemgetter(5))
  valshape =property(itemgetter(6))
  addr=property(itemgetter(7))
  def __new__(cls,ibeg,iend,nrec,
              idxtype,idxshape,valtype,valshape,addr):
    return tuple.__new__(cls, (ibeg,iend,nrec,idxtype,idxshape,valtype,valshape,addr))
  def getidxfn(self,dirname):
    return os.path.join(dirname,'%d_idx.npy'%self.addr)
  def getvalfn(self,dirname):
    return os.path.join(dirname,'%d_val.npy'%self.addr)
  def loadidx(self,dirname,skip=None):
    out=np.load(self.getidxfn(dirname))
    if skip:
      out=out[::skip]
    return out
  def loadval(self,dirname,skip=None):
    out=np.load(self.getvalfn(dirname))
    if skip:
      out=out[::skip]
    return out
  def load(self,dirname,skip=None):
    return self.loadidx(dirname,skip=skip),self.loadval(dirname,skip=skip)
  def checksorted(self,dirname):
    idx= self.loadidx(dirname)
    if np.any(np.diff(idx)<=0):
       raise ValueError,"page %s not ordered"%self.addr
  @classmethod
  def writenew(cls,idx,val,dirname,addr):
    idx1=idx[0]
    idx2=idx[-1]
    nrec=len(idx)
    idxtype=idx[0].dtype.str
    idxshape=idx[0].shape
    valuetype=val[0].dtype.str
    valueshape=val[0].shape
    c=cls(idx1,idx2,nrec,idxtype,idxshape,valuetype,valueshape,addr)
    idxname=c.getidxfn(dirname)
    valname=c.getvalfn(dirname)
    np.save(idxname,idx)
    np.save(valname,val)
    return c


def _merge(ai,av,bi,bv):
  """merge inx and val
     return merged array and discarded values
  """
  mi,mv,di,do,dn=[],[],[],[],[] #merged, discarded
  la,lb=len(ai),len(bi)
  i=j=0
  while i<la and j<lb:
    aii=ai[i]
    bij=bi[j]
    if aii<bij:
      mi.append(aii)
      mv.append(av[i])
      i+=1
    elif aii>bij:
      mi.append(bij)
      mv.append(bv[j])
      j+=1
    else: #aii==bij
      mi.append(bij)
      mv.append(bv[j])
      isconflict=np.any(av[i]!=bv[j])
      if isconflict:
        di.append(bij)
        do.append(av[i])
        dn.append(bv[j])
      i+=1
      j+=1
  while (i<la):
    mi.append(ai[i])
    mv.append(av[i])
    i+=1
  while (j<lb):
    mi.append(bi[j])
    mv.append(bv[j])
    j+=1
  return map(np.array,(mi,mv,di,do,dn))


from objdebug import ObjDebug

class IdxStore(ObjDebug):
  _indexname='000indexj'
  def _getindexfn(self):
    return os.path.join(self.dirname,self.indexname)
  def _getindexoldfn(self):
    timestamp=time.strftime('%Y%m%dT%H%M%S%z')
    return os.path.join(self.dirname,'000indexj_'+timestamp)
  def _loadindex(self):
    meta=simplejson.load(open(self._getindexfn()))
    meta['index']=[ Page(*c) for c in meta['index']]
    self.__dict__.update(meta)
  def _writeindex(self,savecopy=False):
    self.index.sort()
    self.length=self.count()
    attrs=['name','index','length','maxpagesize','desc','lastaddr']
    meta=dict( (k,getattr(self,k)) for k in attrs)
    indexfn=self._getindexfn()
    if savecopy:
      shutil.copy2(indexfn,self._getindexoldfn())
    simplejson.dump(meta,open(indexfn,'w'))
  def _checkindex(self):
    for c in self.index:
      if c.ibeg>c.iend:
        raise ValueError,"page not correct"
      if c.nrec>1:
        c.checksorted(self.dirname)
    for i in range(len(self.index)-1):
      if self.index[i].iend>=self.index[i+1].ibeg:
         print self.index[i]
         print self.index[i+1]
         raise ValueError,"index not ordered"
  def _get_used_pages(self):
    return [page.addr for page in self.index]
  def _get_all_pages(self):
    fnames=glob(os.path.join(self.dirname,'*_idx.npy'))
    return [int(os.path.basename(fn).split('_idx.npy')[0]) for fn in fnames]
  def _get_all_indexes(self):
    fnames=glob(os.path.join(self.dirname,'000indexj_*'))
    return fnames
  def _newaddr(self):
    self.lastaddr+=1
    return self.lastaddr
  def _getlockfn(self):
    return os.path.join(self.dirname,'lock')
  def _lock(self,msg='lock'):
    if self.readonly:
      raise IOError, "IdxStore: attempt to %s while readonly is set"%msg
    nsec=10
    name='IdxTable("%s")'%self.dirname
    lockfn=self._getlockfn()
    res=os.path.exists(lockfn)
    while(res):
      self._debug(0,"%s waitlock... sleeping %dsecs"%(name,nsec))
      time.sleep(nsec)
      res=os.path.exists(lockfn)
    open(self._getlockfn(),'w').write(msg)
  def _unlock(self):
    os.unlink(self._getlockfn())
  def __repr__(self):
    out=['IdxTable: %s(%d)'%(self.name,self.length)]
    return '\n'.join(out)
  def _debug(self,level,*msg):
    if level<self.debug:
      print "IdxStore: ",' '.join(map(str,msg))
  def _rebalance(self,res,remerge=True,resplit=True):
    """regenerate database using a list of
       idx,val or None,Page
    """
    cmax=self.maxpagesize
    index=[]
    while res:
      ai,av=res.pop(0)
      if ai is None:
        pagesize=av.nrec*np.dtype(av.valtype).itemsize
        an=av.nrec
      else:
        pagesize=ai.nbytes
        an=len(ai)
      if remerge and pagesize<cmax/2. and len(res)>0: # merge with next
        if ai is None:
          ai,av=av.load(self.dirname)
        bi,bv=res.pop(0)
        if bi is None:
          bi,bv=bv.load(self.dirname)
        try:
          ci=np.concatenate([ai,bi],axis=0)
          cv=np.concatenate([av,bv],axis=0)
          res.insert(0,[ci,cv])
        except ValueError:
          self._debug(0,"rebalance: could not merge page")
          res.insert(0,[ai,av])
          res.insert(1,[bi,bv])
      elif resplit and pagesize>cmax: # split
        if ai is None:
          ai,av=av.load(self.dirname)
        cut=an/2
        bi,bv=ai[cut:],av[cut:]
        ai,av=ai[:cut],av[:cut]
        res.insert(0,[ai,av])
        res.insert(1,[bi,bv])
      else: #good size
        if ai is None:
          index.append(av)
        else:
          page=Page.writenew(ai,av,self.dirname,self._newaddr())
          self._debug(0,"write page %s"%page.addr)
          index.append(page)
    self.index=index
    self._writeindex(savecopy=True)

  def __init__(self,location,name=None,desc=None,maxpagesize=1000000,debug=0):
    """IdxStore
      dirname: directory where to store data (it will created if not exists
      name: user friendly name, dirnmane is used if None
      description: user friendly description
    """
    self.debug=debug
    if os.path.isfile(location):
      self.dirname,self.indexname=os.path.split(location)
      self._loadindex()
      self.readonly=True
    elif os.path.isdir(location):# db exists fail if index not found
      self.dirname=location
      self.indexname=self._indexname
      indexfullname=self._getindexfn()
      if not os.path.isfile(indexfullname):
        raise IOError, "IdxStore: %s not found" %indexfullname
      self._loadindex()
      self.readonly=False
    elif not os.path.exists(location):
      self.dirname=location
      self.indexname=self._indexname
      os.mkdir(self.dirname)
      self.lastaddr=0
      self.desc=desc
      self.index=[]
      self.readonly=False
      if name is None:
        self.name=self.dirname
      else:
        self.name=name
      if desc is not None:
        self.desc=desc
      else:
        self.desc=''
      self.maxpagesize=maxpagesize
      self._writeindex()
  def reload(self):
    """Reload index"""
    self._loadindex()
    return self
  def get(self,idx1=None,idx2=None,nmax=None,skip=None,concat=True):
    """Return the array of index and record between idx1 and idx2 included
    if idx1 or idx2 is None the first or the last record is used
    if nmax is set only the first nmax records are returned
    """
    idxlist=[]
    vallist=[]
    cnt=0
    if len(self.index)>0:
      if idx1 is None:
        idx1=self.first()
      if idx2 is None :
        idx2=self.last()
      for page in self.index:
        if idx1<=page.ibeg and idx2>=page.iend:
          idx,val=page.load(self.dirname,skip=skip)
          idxlist.append(idx)
          vallist.append(val)
          cnt+=len(idx)
        elif (idx1>=page.ibeg and idx1<=page.iend) or \
             (idx2>=page.ibeg and idx2<=page.iend):
          idx,val=page.load(self.dirname,skip=skip)
          mask=(idx>=idx1)&(idx<=idx2)
          cnt+=sum(mask)
          idxlist.append(idx[mask])
          vallist.append(val[mask])
        if nmax is not None and cnt>nmax:
          idxlist[-1]=idxlist[-1][:nmax-cnt]
          vallist[-1]=vallist[-1][:nmax-cnt]
          break
    if concat is True:
      if len(idxlist)>0:
        try:
          idx=np.concatenate(idxlist,axis=0)
          val=np.concatenate(vallist,axis=0)
        except ValueError:
          print "Error concatenating record of different type"
          print "try usinng contact=False"
          raise ValueError,"np.concatenate arrays must have same number of dimensions"
      else:
        idx,val=None,None
      return idx,val
    else:
      return idxlist,vallist
  def getidx(self,idx1=None,idx2=None,nmax=None,skip=None):
    """Return the array of index between idx1 and idx2 included
    if idx1 or idx2 is None the first or the last record is used
    if nmax is set only the first nmax records are returned
    """
    idxlist=[]
    cnt=0
    if len(self.index)>0:
      if idx1 is None:
        idx1=self.first()
      if idx2 is None :
        idx2=self.last()
      for page in self.index:
        if idx1<=page.ibeg and idx2>=page.iend:
          idx=page.loadidx(self.dirname,skip=skip)
          cnt+=len(idx)
          idxlist.append(idx)
        elif (idx1>=page.ibeg and idx1<=page.iend) or \
             (idx2>=page.ibeg and idx2<=page.iend):
          idx=page.loadidx(self.dirname,skip=skip)
          mask=(idx>=idx1)&(idx<=idx2)
          cnt+=sum(mask)
          idxlist.append(idx[mask])
        if nmax is not None and cnt>nmax:
          idxlist[-1]=idxlist[-1][:nmax-cnt]
          break
    if len(idxlist)>0:
      idx=np.concatenate(idxlist,axis=0)
    return idx
  def first(self):
    """Return first index"""
    if len(self.index)>0:
      return self.index[0][0]
  def last(self):
    """Return last index"""
    if len(self.index)>0:
      return self.index[-1][1]
  def count(self,idx1=None,idx2=None,skip=None):
    """Count the number of records between idx1 and idx2 included
    if idx1 or idx2 is None the first or the last record is used
    """
    cnt=0
    if len(self.index)>0:
      if idx1 is None:
        idx1=self.first()
      if idx2 is None :
        idx2=self.last()
      for page in self.index:
        if idx1<=page.ibeg and idx2>=page.iend:
          cnt+=page.nrec
        elif (idx1>=page.ibeg and idx1<=page.iend) or \
             (idx2>=page.ibeg and idx2<=page.iend):
            idx=page.loadidx(self.dirname,skip=skip)
            mask=(idx>=idx1)&(idx<=idx2)
            cnt+=sum(mask)
    return cnt
  def remove(self,idx1,idx2):
    """Remove  all data between idx1 and idx2 included
    if idx1 or idx2 is None the first or the last record is used
    """
    if len(self.index)>0:
      self._lock('remove')
      if idx1 is None:
        idx1=self.first()
      if idx2 is None :
        idx2=self.last()
      res=[]
      for page in self.index:
        if page.iend>=idx1 or page.ibeg<=idx2:
          idx,val=page.load(self.dirname)
          mask=(idx<idx1)|(idx>idx2)
          nidx=idx[mask]
          if len(nidx)>0:
            res.append([nidx,val[mask]])
        else:
          res.append([None,page])
      print res
      self._rebalance(res)
      self._unlock()
    return self
  def store(self,idx,val):
    self._lock('store')
    self._loadindex()
    oldindex=self.index[:]
    newdata=[[idx[0],idx[-1],idx,val]]
    coll=[]
    res=[]
    while(len(newdata)>0 and len(oldindex)>0):
      page=oldindex.pop(0)
      a1,a2=page.ibeg,page.iend
      b1,b2,bi,bv=newdata.pop(0)
      self._debug(1,(a1,a2,b1,b2))
      if b2<a1:
        self._debug(1,"no overlap b<a")
        res.append([bi,bv])
        res.append([None,page])
      elif a2<b1:
        self._debug(1,"no overlap a<b")
        res.append([None,page])
        newdata.insert(0,[b1,b2,bi,bv])
      else: # manage conflict
        self._debug(1,"conflict",)
        il,ir=0,b2+1
        if b1<a1: #print "a1 cut b",
          il=bi.searchsorted(a1)
          bbl=bi[:il]
          self._debug(1,"insert",bbl[0],bbl[-1])
          res.append([bbl,bv[:il]])
        if b2>a2: #print "a2 cut b",
          ir=bi.searchsorted(a2)
          if bi[ir]==a2:
            ir+=1
          #ir=findgt(bi,a2)
          bbr=bi[ir:]
          if len(bbr)>0:
            self._debug(1,"reprocess",bbr[0],bbr[-1])
            newdata.insert(0,[bbr[0],bbr[-1],bbr,bv[ir:]])
        bbc=bi[il:ir]
        bbcv=bv[il:ir]
        if len(bbc)>0:
          ai,av=page.load(self.dirname)
          self._debug(1,"merge",(ai[0],ai[-1],bbc[0],bbc[-1]))
          mi,mv,di,do,dn=_merge(ai,av,bbc,bbcv)
          res.append([mi,mv])
          if len(di)>0:
            self._debug(0,"Found %d conflicting values"%len(di))
            self._debug(0,"di",di)
            self._debug(0,"do",do)
            self._debug(0,"dn",dn)
            coll.append([di,do,dn])
    if len(newdata)>0:
      for b1,b2,bi,bv in newdata:
        self._debug(1,"still newdata",b1,b2)
        res.append([bi,bv])
    if len(oldindex)>0:
      for page in oldindex:
        self._debug(1,"still oldindex",page.ibeg,page.iend)
        res.append([None,page])
    self._rebalance(res)
    self._unlock()
    #for ai,av in res:
    #  if ai is None:
    #    print av.ibeg,av.iend
    #  else:
    #    print ai[0],ai[-1]
    return self
  def sync(self,maxpagesize=None):
    self._lock('sync')
    if maxpagesize is not None:
      self.maxpagesize=maxpagesize
    res=[ [None,c] for c in self.index]
    self._rebalance(res)
    self._unlock()
    return self
  def purge(self):
    used=set(self._get_used_pages())
    for addr in self._get_all_pages():
      if addr not in used:
        idxfn=os.path.join(self.dirname,'%d_idx.npy'%addr)
        valfn=os.path.join(self.dirname,'%d_val.npy'%addr)
        print idxfn
        print valfn
        os.unlink(idxfn)
        os.unlink(valfn)
    for fn in self._get_all_indexes():
      print fn
      os.unlink(fn)
