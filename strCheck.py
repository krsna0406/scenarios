# class ClsONe:
#     """ this is clsone"""
#     def __init__(self):
#         self.a=100
#         self.b=200
#     @classmethod
#     def clsM1(cls):
#         cls.a2=100
#
#
#
# help(ClsONe)
#
# print(ClsONe.__doc__)
# obj=ClsONe()
# obj.d=50
# print(obj.__dict__)
import builtins
# import datetime
# today=datetime.datetime.now()
# print(today)
#
# print(str(today))
#
#
# print(eval(repr(today)))



#
# try:
#     print("0/10",10/0)
# except (ZeroDivisionError ,ValueError)as e:
#     print("except error",e)
#
# try:
#     print("try")
#     print(10/0)
# except NameError:
#     print("except")
# finally:
#     print("finally")

#
#
# import logging
# logging.basicConfig(filename='log.txt',level=logging.WARNING)
# print("logging demo")
# logging.debug('Debug Information')
#
# def square(num):
#     return num*num+1
#
# assert square(2)==4, 'squre of 2 is 4'


# import keyword
# print(keyword.kwlist)
#
# print(dir(builtins))

# s={}
#
# print(type(s))
#
# print('ab' and 'xz')
#
# a,b,c=20,30,10
# min=a if a<b and a<c else b if b<c else c
#
# import math
# print(math.sqrt(90))


from sys import argv

print(type(argv))