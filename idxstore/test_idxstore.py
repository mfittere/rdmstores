from idxstore import *
from numpy import *


os.system('rm -r test1')
db=IdxStore('test1')
db.get(0,100)

a=[[1,5],
   [7,13],
   [15,20],
   [25,33],
   [35,40]]

b=[[ 10, 30 ],
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

for i,v in zip(*db.get(nmax=None)):
  print i,v


base='/home/rdemaria/pcbe13028/lhcbbqdata/tetra_test/data/*.csv.gz'
fnames=sorted(glob(base))

def sanitize(s):
  return "".join(x for x in s if x.isalpha() or x.isdigit())

for fn in fnames:
  print fn
  data=cernlogdb.open(fn)
  for table in data['datavars']:
    dirname=os.path.join('test2',sanitize(table))
    print dirname
    db=IdxStore(dirname,name=table)
    idx,val=data[table]
    db.set(idx,val)

