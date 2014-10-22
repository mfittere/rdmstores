"""""

a on disk
b in memory

Loop for 6 cases

a      |----------------|
b |-|                       |--|
b     |--|   |--|     |---|
b     |-------------------|

Then indentify overlap and change a in one or more
dataset in memory

c is a list of memory arrays and reference on disk
then join and split
join and splis




"""""



def prv(a,l=''):
  print l
  for a1,a2,av in a:
    print "%4d, %4d, %6g...%6g"%(a1,a2,av[0],av[-1])


def prc(a,l=''):
  print l
  for a1,a2,s,id1,id2 in a:
    print "%4d, %4d, %s: %4d, %4d"%(a1,a2,s,id1,id2)

def findlt(lst,v):
  # find first i for which lst[i] > v
  i=0
  for a in lst:
    if a>=v:
      break
    i+=1
  return i


def merge(a,b):
  prv(a,'a')
  prv(b,'b')
  c=[]
  o=[]
  cmd=[]
  while( len(b)>0 and len(a)>0 ):
    a1,a2,av=a.pop(0)
    b1,b2,bv=b.pop(0)
    #print '>>>',('%4d'*4)%(a1,a2,b1,b2,),
    if b2<a1: #print "no overlap b<a"
      c.append([b1,b2,bv])
      cmd.append([b1,b2,'b',])
      a.insert(0,[a1,a2,av])
    elif a2<b1: #print "no overlap a<b"
      c.append([a1,a2,av])
      b.insert(0,[b1,b2,bv])
    else: # mange conflicts favoring a
      il,ir=0,b2+1
      if b1<a2 and a2<b2: #print "a2 cut b",
        ir=findlt(bv,a2)+1
        bbr=bv[ir:] #print "insert",bbr[0],bbr[-1]
        b.insert(0,[bbr[0],bbr[-1],bbr])
      if b1<a1 and a1<b2: #print "a1 cut b",
        il=findlt(bv,a1)
        bbl=bv[:il] #print "insert",bbl[0],bbl[-1]
        b.insert(0,[bbl[0],bbl[-1],bbl])
      a.insert(0,[a1,a2,av])
      bbc=bv[il:ir]
      o.append([bbc[0],bbc[-1],bbc])
  if len(b)>0:
    c.extend(b)
  if len(a)>0:
    c.extend(a)
  prv(c,'c')
  prv(o,'o')


a=[[ 35, 96 ],
   [840, 916]]

b=[[3,9],
   [20,40],
   [50,60],
   [90,100],
   [950,960]]

def mkv(a,seed=0):
  return [ [a1,a2,arange(a1,a2+1)+seed] for a1,a2 in a]

a,b=mkv(a),mkv(b)

maxsize=10
merge(b,a)







def mergeval(a,b):
  la=len(a)
  lb=len(b)
  c=[]
  i=0
  j=0
  while (i<la and j<lb):
    av=a[i]
    bv=b[j]
    if av<bv:
      c.append(['a',i])
      i+=1
    elif av>bv:
      c.append(['b',j])
      j+=1
    elif av==bv:
      c.append(['ab',(i,j)])
      i+=1
      j+=1
  while (i<la):
    c.append(['a',i])
    i+=1
  while (j<lb):
    c.append(['b',j])
    j+=1
  return c

def mkval(a,b,ic,):
  c=[]
  o=[]
  for s,i in ic:
    if s=='a':
      c.append(a[i])
    elif s=='b':
      c.append(b[i])
    elif s=='ab':
      c.append(b[i[1]])
      o.append(a[i[0]])
  return c,o


a,b=[1,2,4],[2,7,9]

print a,b
c,o=mkval(a,b,mergeval(a,b))
print c,o

