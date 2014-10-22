from idxstore import *
from numpy import *



os.system('rm -r test1')
db=IdxStore('test1')
db.get(0,100)

a=[[0,5],
   [6,14],
   [15,20],
   [25,34],
   [35,49]]

b=[[ 10, 17 ],
   [ 23, 24 ],
   [ 50, 55]]

def mkv(a,seed=0):
  return [ [arange(a1,a2+1),arange(a1,a2+1)+seed] for a1,a2 in a]

a,b=mkv(a,0.1),mkv(b,0.2)

for idx,val in a:
  db.store(idx,val)

db.debug=100
for idx,val in b:
  #print idx[0],idx[-1]
  db.store(idx,val)

idx,val=db.get()

#for i,v in zip(idx,val):
#  print i,v


for idx,val in b:
  for i,v in  zip(idx,val):
    ii,vv=db.get(i,i)
    try:
      assert vv[0]==v
      assert ii[0]==i
    except AssertionError:
      print 'Error',i,v ,ii,vv

for idx,val in a:
  for i,v in  zip(idx,val):
    ii,vv=db.get(i,i)
    try:
      assert ii[0]==i
    except AssertionError:
      print 'Error',i,v ,ii,vv

