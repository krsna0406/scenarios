lst=[[1,2,(3,4),5,[6,7,8],9]]
# output=[1,2,3,4,5,6,7,8,9]




def flatten(data):
    temp=[]
    for item in data:
        print(item)
        if isinstance(item,(list,tuple)):
            temp.extend(flatten(item))
        else:
            temp.append(item)
    return temp;


print(flatten(lst))





