import os

import numpy as np
import simplejson

from rawdata import RawData

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

def _multuple(t):
  reduce(lambda a,b: a*b, t)

class Page(object):
  def __init__(self,idxbeg,idxend,length,data):
    self.idxbeg=idxbeg
    self.idxend=idxend
    self.length=length
    self.data=data
  def __repr__(self):
    return "Page(%d,%d,%d,%d)"%(self.idxbeg,self.idxend,self.length,self.data.uid)
  def split_array(page,arr):
    if arr[0]>page.idxend:
      iileft=0
      iiright=0
    elif arr[-1]<page.idxbeg:
      iileft=len(arr)
      iiright=len(arr)
    else:
      iileft=0
      iiright=0
      for ii,idx in enumerate(arr):
        if idx<=page.idxbeg:
          iileft=ii
        if idx<=page.idxend:
          iiright=ii+1
    return iileft,iiright
  def toJson(self):
    return [self.idxbeg,self.idxend,self.length,self.data.toJson()]

class IdxStore(object):
  maxpagesize=50000000
  _indexname="000indexj"
  def __init__(self,basedir):
    if os.path.isfile(basedir):
      self.basedir,self.indexname=os.path.split(basedir)
      self._load_index()
    elif os.path.isdir(basedir):
      self.basedir=basedir
      self.indexname=IdxStore._indexname
      self._load_index()
    elif not os.path.exists(basedir):
      self._newindex(basedir)
    else:
      raise ValueError,"Cannot open %s"%self.basedir
  def _new_uid(self):
    return self.lastaddr+1
  def _get_indexfn(self):
    return os.path.join(self.basedir,self.indexname)
  def _newindex(self,basedir):
      self.basedir=basedir
      self.indexname=IdxStore._indexname
      os.mkdir(self.basedir)
      self.lastaddr=0
      self.index=[]
      self.name=basedir
      self._write_index()
  def _load_index(self):
    meta=simplejson.load(open(self._get_indexfn()))
    meta['index']=[ self._page_from_index(*c) for c in meta['index']]
    self.__dict__.update(meta)
  def _write_index(self):
    self.length=self.count()
    meta={'name':self.name,'maxpagesize':self.maxpagesize,
        'lastaddr':self.lastaddr}
    meta['index']=[ p.toJson() for p in self.index]
    simplejson.dump(meta,open(self._get_indexfn(),'w'))
  def store(self,idx,val):
    curr=-1
    while len(idx)>0:
      curr+=1
      # get left
      if curr==len(self.index):
        newpage=self._page_from_data(idx,val)
        self.index.append(newpage)
        break
      page=self.index[curr]
      #print page,idx[0],idx[-1]
      iileft,iiright=page.split_array(idx)
      #print 'l,r',iileft,iiright
      idxleft=idx[:iileft]
      if len(idxleft)>0:
        newpage=self._page_from_data(idxleft,val[:iileft])
        #print "insert", newpage
        self.index.insert(curr,newpage)
        curr+=1
      idxover=idx[iileft:iiright]
      valover=val[iileft:iiright]
      if len(idxover)>0:
        pageidx=page.data.load_idx(self.basedir)
        pageval=page.data.load_val(self.basedir)
        midx,mval,sidx,ovals,nvals=_merge(pageidx,pageval,idxover,valover)
        newpage=self._page_from_data(midx,mval)
        self.index[curr]=newpage
      idx=idx[iiright:]
      val=val[iiright:]
    self.rebalance()
  def rebalance(self):
    #print 'before split',self.index
    index=[]
    cmax=self.maxpagesize
    while self.index:
      page=self.index.pop(0)
      if page.data.valsize>cmax:
        pages=self._page_split(page)
        index.extend(pages[:-1])
        page=pages[-1]
      count=page.data.valsize
      pages=[page]
      #print 'after split',index,pages,count,self.index
      while self.index:
        ahead=self.index[0]
        aheads=ahead.data.valsize
        if count+aheads<cmax and page.data.can_cat_with(ahead.data):
          count+=aheads
          pages.append(self.index.pop(0))
        else:
          break
      #print 'after merge',index,pages,count,self.index
      if len(pages)==1:
        index.append(pages[0])
      else:
        index.append(self._page_merge(pages))
    self.index=index
    self._write_index()
  def _page_split(self,page):
    idx,val=page.data.load(self.basedir)
    out=[]
    recsize=float(page.data.valsize/len(val))
    #print recsize
    cmin=int(round(.5*self.maxpagesize/recsize))
    #print cmin,len(idx)
    while len(idx)>0:
      newpage=self._page_from_data(idx[:cmin],val[:cmin])
      out.append(newpage)
      idx=idx[cmin:]
      val=val[cmin:]
    return out
  def _page_merge(self,pages):
    if len(pages)==1:
      return pages[0]
    else:
      vals=[page.data.load_val(self.basedir) for page in pages]
      idxs=[page.data.load_idx(self.basedir) for page in pages]
      val=np.concatenate(vals,axis=0)
      idx=np.concatenate(idxs,axis=0)
      return self._page_from_data(idx,val)
  def _page_from_data(self,idx,val):
    uid=self._new_uid()
    data=RawData(uid,idx.dtype,idx.shape,val.dtype,val.shape)
    data.store(idx,val,self.basedir)
    self.lastaddr=uid
    return Page(idx[0],idx[-1],len(idx),data)
  def _page_from_index(self,idxbeg,idxend,length,data):
    if data[0]=="RawData":
      data=RawData(*data[1:])
    return Page(idxbeg,idxend,length,data)
  def first(self):
    return self.index[0].idxbeg
  def last(self):
    return self.index[-1].idxend
  def count(self,idx1=None,idx2=None,nmax=None,skip=None):
    """Return the array of index and record between idx1 and idx2 included
    if idx1 or idx2 is None the first or the last record is used
    if nmax is set only the first nmax records are returned
    """
    cnt=0
    if len(self.index)>0:
      if idx1 is None:
        idx1=self.first()
      if idx2 is None :
        idx2=self.last()
      for page in self.index:
        if idx1<=page.idxbeg and idx2>=page.idxend:
          cnt+=page.length
        elif (idx1>=page.idxbeg and idx1<=page.idxend) or \
             (idx2>=page.idxbeg and idx2<=page.idxend):
          idx=page.data.load_idx(self.basedir,skip=skip)
          mask=(idx>=idx1)&(idx<=idx2)
          cnt+=sum(mask)
        if nmax is not None and cnt>nmax:
          break
    return cnt
  def get(self,idx1=None,idx2=None,nmax=None,skip=None,idxonly=False):
    """Return the array of index and record between idx1 and idx2 included
    if idx1 or idx2 is None the first or the last record is used
    if nmax is set only the first nmax records are returned
    """
    idxlist=[]
    if not idxonly:
      vallist=[]
    cnt=0
    if len(self.index)>0:
      if idx1 is None:
        idx1=self.first()
      if idx2 is None :
        idx2=self.last()
      for page in self.index:
        if idx1<=page.idxbeg and idx2>=page.idxend:
          idx=page.data.load_idx(self.basedir,skip=skip)
          idxlist.append(idx)
          if not idxonly:
            val=page.data.load_val(self.basedir,skip=skip)
            vallist.append(val)
          cnt+=len(idx)
        elif (idx1>=page.idxbeg and idx1<=page.idxend) or \
             (idx2>=page.idxbeg and idx2<=page.idxend):
          idx=page.data.load_idx(self.basedir,skip=skip)
          mask=(idx>=idx1)&(idx<=idx2)
          cnt+=sum(mask)
          idxlist.append(idx[mask])
          if not idxonly:
            val=page.data.load_val(self.basedir,skip=skip)
            vallist.append(val[mask])
        if nmax is not None and cnt>nmax:
          idxlist[-1]=idxlist[-1][:nmax-cnt]
          if not idxonly:
            vallist[-1]=vallist[-1][:nmax-cnt]
          break
    if len(idxlist)>0:
      idx=np.concatenate(idxlist,axis=0)
      if not idxonly:
        val=np.concatenate(vallist,axis=0)
    else:
      idx,val=None,None
    if not idxonly:
      return idx,val
    else:
      return idx
  def remove(self,idx1=None,idx2=None):
    """Remove  all data between idx1 and idx2 included
    if idx1 or idx2 is None the first or the last record is used
    """
    if len(self.index)>0:
      if idx1 is None:
        idx1=self.first()
      if idx2 is None :
        idx2=self.last()
      index=[]
      while self.index:
        page=self.index.pop(0)
        if (idx1>=page.idxbeg and idx1<=page.idxend) or \
           (idx2>=page.idxbeg and idx2<=page.idxend):
          idx=page.data.load_idx(self.basedir,skip=skip)
          mask=(idx<idx1)|(idx>idx2)
          nidx=idx[mask]
          if len(nidx)>0:
             val=page.data.load_val(self.basedir,skip=skip)
             index.append(self._page_from_data(nidx,val[mask]))
        elif page.idxend>=idx1 or page.idxbeg<=idx2:
          index.append(page)
      self.index=index
      self.rebalance()
  def __repr__(self):
    return "<IdxStore %s %s>"%(self.name,self.count())
  def prune(self):
    pages=set(RawData.get_allpages(self.basedir))
    for p in self.index:
      pages.remove(p.data.uid)
    #print pages
    for uid in pages:
      p=RawData(uid,int,(0,),int,(0,))
      p.remove(self.basedir)
  def sync(self):
    self._load_index()


