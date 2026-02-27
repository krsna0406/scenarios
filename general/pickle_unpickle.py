import pickle

print("pickling and unpickling")

#create one object and write the state to file and vice versa


class Employee:
    def __init__(self,eno,ename,esal,eaddr):
        print("employee object created")
        self.eno=eno
        self.ename=ename
        self.esal=esal
        self.eaddr=eaddr

    def display(self):
        print(self.eno,self.ename,self.esal,self.eaddr)

with open("emp.dat","wb") as f:
    e=Employee(100,"Durga",1000,"Hyd")
    pickle.dump(e,f)
    print("Pickling of Employee Object completed...")

with open("emp.dat","rb") as f:
        obj=pickle.load(f)
        obj.display()
        print("Printing Employee Information after unpickling")

## generator

def fib():
    a,b=0,1
    while True:
        yield a
        a,b=b,a+b

fnos=fib()

for i in fnos:
    if i<100:
        print(i,sep=" - ")
