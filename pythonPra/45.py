
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



