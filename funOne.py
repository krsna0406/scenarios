# def outer():
#     print("outer function")
#
#     def inner():
#         print("inner function")
#
#     return inner
#
# f1 = outer();
# f1()

# DECORATOR
#
# def decor(func):
#     def inner_fun(name):
#         print("called inner function")
#         if name=='krishna':
#             print("calling original function")
#             func(name)
#         elif name=="ilavar":
#             print("HIIIIII",name)
#
#     return inner_fun
#
# @decor
# def wish(name):
#     print("HI",name)
#
#
# wish("ilavar")


# example 2 decorator


# def addDecor(func):
#     def inner(a, b):
#         print('#' * 30)
#         print("the sum is ", end='')
#         func(a, b)
#         print('#' * 30)
#     return  inner
#
#
# @addDecor
# def add(a, b):
#     print(a + b)
#
#
# add(10, 20)


# checking without @

def addDecor(func):
    # def inner(a):# failed as given one arg
    def inner(a,b):
        print('#' * 30)
        print("the sum is ", end='')
        func(a, b)
        print('#' * 30)
    return inner


def add(a, b):
    print(a + b)


decorAdd = addDecor(add)

add(10, 20)
decorAdd(10, 40)
