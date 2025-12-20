# n=int(input("enter the no ....."))
# n=5
# # for i in range(n):
# #     # print("* "*n)
# #     #print((chr(65+i)+' ')*n)
# #     #print((str(i+1)+' ')*n)
# #
#
# # right angle triangle
# for i in range(n):
#     print("* "* (i+1))
#
# print
# print
# # invert right angle triangle
# for i in range(n):
#     print("* " * (n - i))


# pyramid
#
# n=4
# for i in range(n):
#     print(" "*(n-(i+1)) + "* "*(i+1))
#
#
#
# # inverted pyramid
#
# n=4
# for i in range(n-1):
#     print(" "*(i) + "* "*(n-i))
#


# daimand

n=4
for i in range(n):
    print(" "*(n-(i+1)) + "* "*(i+1)) #n-i-1

for i in range(n-1):
        print(" "*(i+1) + "* "*(n-(i+1))) #n-i-1