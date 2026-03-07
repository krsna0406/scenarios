# nums = [8,4,5,6,7,5,9,3]
# target = 12
#
# seen = set()
#
# for n in nums:
#     print("target and number",target,n)
#     diff = target - n
#
#     if diff in seen:
#         print(diff, n)
#
#     seen.add(n)


############################
#
# s = "santoshmishra"
#
# d={}
# for temp in s:
#     d[temp]=d.get(temp,0)+1
# print(d)
# for k,v in d.items():
#     print("{} is {}".format(k,v))
#
#
# # by using counters
# from collections import Counter
# print(Counter(s))
# print(Counter(s).items())
# for k,v in Counter(s).items():
#     print(k,v)
#


# l=[2, 3, 6, 2, 10,11,1 ]
#
# print(max(l))
#
# s = "a1hec3y4e5"
#
# pairs = []
# i = 0
#
# while i < len(s):
#     if i+1 < len(s) and s[i+1].isdigit():
#         pairs.append(s[i] + s[i+1])
#         i += 2
#     else:
#         pairs.append(s[i])
#         i += 1
#
# print("".join(pairs[::-1]))
#
# words = ["abc","abca","abcd","aabb","xyz","xyzz"]
#
# res = []
#
# for w in words:
#     if len(w) == len(set(w)):   # checks if characters repeat
#         res.append(w)
#
# max_len = max(len(w) for w in res)
#
# output = [w for w in res if len(w) == max_len]
#
# print(output)


# output
# 1234
# 123
# 12
# 1



n=4

for i in range(n,0,-1):
    tempStr=''
    for j in range(1,i+1):
        tempStr+=str(j)
    print(tempStr)


