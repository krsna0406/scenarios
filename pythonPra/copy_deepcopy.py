import copy

l=[[1,2,3],[4,5,6]]

l1=l

print(l1) # aliasing



l2=copy.copy(l)

l3=copy.deepcopy(l)

l[0][1]=100

print(l2)
print(l3)





