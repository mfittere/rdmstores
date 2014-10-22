def insert(data):
  curr=-1
  while len(data)>0:
    curr+=1
    # get left
    if curr==len(db):
      db.append(Page.fromdata(data))
      break
    page=db[curr]
    dataleft,dataover,data=page.splitdata(data)
    if len(dataleft)>0:
      db.insert(curr,Page.fromdata(dataleft))
      curr+=1
    if len(dataover)>0:
      merged,conflicts=page.merge(dataover)
      print conflicts
      db[curr]=Page.fromdata(merged)
  return db

class Page(object):
  def __init__(self,idxbegin,idxend,length,data):
    self.idxbegin=idxbegin
    self.idxend=idxend
    self.length=length
    self.data=data
  def __repr__(self):
    return "Page(%d,%d,%d)"%(self.idxbegin,self.idxend,self.length)
  @classmethod
  def fromdata(cls,data):
    return cls(data[0],data[-1],len(data),data)
  def splitdata(page,data):
    iileft=0
    iiright=0
    if data[-1]<page.idxbegin:
      return data,[],[]
    elif data[0]>page.idxend:
      return [],[],data
    else:
      for ii,idx in enumerate(data):
        if idx<page.idxbegin:
          iileft=ii+1
        if idx<=page.idxend:
          iiright=ii+1
      return data[:iileft],data[iileft:iiright],data[iiright:]
  def merge(self,data):
    new=[]
    old=[]
    ipage=0
    idata=0
    page=self.data
    while ipage<len(page) or idata<len(data):
      if ipage==len(page):
        idxdata=data[idata]
        new.append(idxdata)
        idata+=1
      elif idata==len(data):
        idxpage=page[ipage]
        new.append(idxpage)
        ipage+=1
      else:
        idxdata=data[idata]
        idxpage=page[ipage]
        if idxdata<idxpage:
          new.append(idxdata)
          idata+=1
        elif idxdata==idxpage:
          new.append(idxdata)
          old.append(idxpage)
          idata+=1
          ipage+=1
        else:
          new.append(idxpage)
          ipage+=1
    return new,old



a=range(4,6)
b=range(9,12)
c=range(12,14)
d=range(18,25)
e=range(8,22)
v=range(10,20)
z=range(30,40)

# cases
# |-a-| |-b-|  |-c-|  |-d-|
# |-------------e-----------|
#         |-----v--------|      |-----z-----|


#  while there is 


page=Page.fromdata(v)
assert page.splitdata(a)==([4, 5], [], [])
assert page.splitdata(b)==([9], [10, 11], [])
assert page.splitdata(c)==([], [12, 13], [])
assert page.splitdata(d)==([], [18,19], [20, 21, 22, 23, 24])
assert page.splitdata(z)==([], [], [30, 31, 32, 33, 34, 35, 36, 37, 38, 39])

db=[]
insert(v)
insert(a)
insert(b)
insert(c)
insert(d)
insert(z)






