import idxstore
import os
from numpy import *


reload(idxstore)
p=idxstore.Page(5,10,40,None)

def split(p,arr):
  l,r=p.split_array(arr)
  return arr[:l],arr[l:r],arr[r:]

assert split(p,[3,4])==([3, 4], [], [])
assert split(p,[4,5,6])==([4], [5, 6], [])
assert split(p,[6,7,8,9])==([], [6, 7, 8, 9], [])
assert split(p,[9,10,11])==([], [9, 10], [11])
assert split(p,[11,12])==([], [], [11, 12])
assert split(p,range(4,12))==([4], [5, 6, 7, 8, 9, 10], [11])
assert split(p,range(5,11))==([], [5, 6, 7, 8, 9, 10], [])



os.system('rm -r test')

reload(idxstore)
db=idxstore.IdxStore('test')
db.__class__=idxstore.IdxStore
db=idxstore.IdxStore('test')
db.maxpagesize=300

db.count()
db.get()

idx=arange(0,400)
val=random.rand(len(idx))
rng=[ (x,x+70) for x in range(0,len(idx),30)]

for i1,i2 in rng:
  cidx=idx[i1:i2];cval=val[i1:i2]
  db.store(cidx,cval)
  a=cidx[0];b=cidx[-1]
  cnt=db.count(a,b)
  didx,dval=db.get(a,b)
  assert cnt==len(cidx)
  assert all(cidx==didx)
  assert all(cval==dval)

db.rebalance()
db.store(cidx,cval)

cnt=db.count()
didx,dval=db.get()
assert cnt==len(idx)
assert all(idx==didx)
assert all(val==dval)




