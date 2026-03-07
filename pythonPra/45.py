
# 1
# # Source = [[1,2,(3,4),5,[6,7,8],9]]
# # Output = [1,2,3,4,5,6,7,8,9]
#
# source=[[1,2,(3,4),5,[6,7,8],9]]
#
# def mone(src):
#     target=[]
#     for x in src:
#         if isinstance(x,(list,tuple)):
#             print("list/tuple" ,x)
#             print('target  ',target)
#             target.extend(mone(x))
#         else:
#             target.append(x)
#     return target
#
# print(mone(source))

#2
#python functions

#3

# Write a Python program using a class where:
#     •	Two numbers are taken from the user
#     •	Stored inside the class
#         •	A class method or instance method is used to print the sum of the two numbers


# class ClsOne:
#     def __init__(self,num1,num2):
#         self.a=num1
#         self.b=num2
#     def mone(self):
#         return self.a+self.b
#
#
# obj=ClsOne(10,20)
# print(obj.mone())


#4
# Generator in python, How can you use it in data pipelines.
#5
#Dictionaries input {a:b} {c:d} output. {a:c,b:d}

d1 = {'a': 'b'}
d2 = {'c': 'd'}

k1, v1 = list(d1.items())[0]
k2, v2 = list(d2.items())[0]

result = {k1: k2, v1: v2}

print(result)

#6 Find the highest and second highest from the list

# Assume a list like:

nums = [10, 25, 8, 40, 32, 40, 15]
# by using set and sort

unique_nums = list(set(nums))
unique_nums.sort(reverse=True)

#imp sort works on list only and modify the same list as it is mutable

print(unique_nums[0]) # 40
print(unique_nums[1]) # 32

# second way

first=second=float('-inf')

for no in nums:
    if no > first:
        first=no
        second=first
    elif no > second and no!= first:
        second=no

print("first {} second {} ".format(first,second))


