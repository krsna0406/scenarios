# lst=[[1,2,(3,4),5,[6,7,8],9]]
# # output=[1,2,3,4,5,6,7,8,9]
#
# def flatten(data):
#     temp=[]
#     for item in data:
#         print(item)
#         if isinstance(item,(list,tuple)):
#             temp.extend(flatten(item))
#         else:
#             temp.append(item)
#     return temp;
#
#
# print(flatten(lst))
#

# data = [
#     ["apple", "banana", "orange"],
#     ["banana", "kiwi", "apple"],
#     ["grape", "banana"]
# ]
#
# print(data)
#
# d1={}
#
# for olist in data:
#     for inner in olist:
#         print(inner)
#         d1[inner]=d1.get(inner,0)+1
#
# print(d1)
#
# for k,v in d1.items():
#     if v>=2:
#         print(k,'  ',v)



lst=[8,4,5,6,7,5,9,3]
# need pair whi gives sum as 12
summ=12
# pairs = set()
# for i in range(len(lst)):
#     for j in range(i+1,len(lst)):
#         if lst[i]+lst[j]==12:
#             # pairs.append((lst[i], lst[j]))
#             pairs.add(tuple(sorted((lst[i], lst[j]))))
#
#     print()
#
# print(pairs)

nums = [8,4,5,6,7,5,9,3]
target = 12

seen = set()
pairs = set()

for num in lst:
    comp = target - num
    if comp in seen:
        pairs.add(tuple(sorted((num, comp))))
    seen.add(num)

print(pairs)