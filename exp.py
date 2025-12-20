#n=eval(input("enter the array"))

# var=[int(x) for x in input("enter the array    ").split()]
#
# #a=[int(x) for x in input("Enter 2 numbers :").split()]
#
# print(var)
# for i in var:
#      print(i)


# while True:
#      print("krishna")


# n=7
# for i in range(1,n+1):
#     print("* "*i)


# cart=[10,20,30,40,50]
# for item in cart:
#     if item>=500:
#         print("We cannot process this order")
#         break
#         print(item)
#
# else:
#         print("Congrats ...all items processed suc")

# list = list(range(1,10))
# print(list)


# def wish(msg,name="kittu"):
#     print("Hi {0} wishing with {1}".format(name,msg))
#
# wish("good morning","aaaaaa")


# print(__name__)


# class ClsOne:
#     """this is my first class in python"""
# print(ClsOne.__doc__)
# help(ClsOne)



# class Student:
#     """this is class student"""
#     def __init__(self):
#         print('calling the constructor')
#         self.varone="one"
#         self.vartwo="two"
#
#     def mOne(self):
#         print(self.varone)
#         print(self.vartwo)
#
# s=Student()
# s.mOne()

print("hello")

try:
    print(10/0)
except ZeroDivisionError as msg:
    print("exception",msg)
finally:
    print("finally block")
