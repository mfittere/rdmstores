from idxstore import *
from numpy import *
import os

def mkdatasparse(nsize,rng):
  idx=unique(random.randint(rng,size=nsize))
  idx.sort()
  val=random.randint(rng*4,size=len(idx))
  return idx,val


def mkdatadense(nsize,rng):
  idx=random.randint(rng,size=2)
  idx.sort()
  idx=linspace(idx[0],idx[1]+1,nsize)
  val=random.rand(len(idx))
  return idx,val



def mktest(nset=20,nsize=20,rng=40,cmax=40,mkdata=mkdatasparse):
  tstart=time.time()
  os.system('rm -r test1')
  db=IdxStore('test1',maxpagesize=cmax)
  store={}
  for iii in range(nset):
    idx,val=mkdata(nsize,rng)
    for i,v in zip(idx,val):
      store[i]=v
    if iii%10==0:
      print 'dataset %3d'%iii
    db.store(idx,val)
    for i,v in store.items():
      eps=1+1e-10
      nidx,nval=db.get(i/eps,i*eps)
      try:
        assert len(nidx)==1
        assert len(nval)==1
        assert nidx[0]==i
        assert nval[0]==v
      except AssertionError:
        print "Error on %s %s, %s %s"%(i,v,nidx,nval)
        if len(nidx)>1:
          print "diff", diff(nidx)
        print idx,val
        return db
    db._checkindex()
  tstop=time.time()
  print
  print 'Done %d %d %d in %.2fsec'%(nsize,cmax,rng,tstop-tstart)
  return db


#db=mktest(100,20,40,40,0,mkdatasparse)
db=mktest(nset=20,nsize=20,rng=40,cmax=40,mkdata=mkdatadense)
#db=mktest(nset=2000,nsize=20,rng=40,cmax=40,mkdata=mkdatasparse,debug=0)



