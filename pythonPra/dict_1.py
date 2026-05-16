# # # # # prime no if number is 0,1 then no prime ,and starts with 2
# # # #
# # # #
# # # # def prime(number):
# # # #     flag=False
# # # #     if number ==0 or number==1:
# # # #         print("no prime number")
# # # #     else:
# # # #         for i in range(2,number+1):
# # # #             if number%2 ==0 :
# # # #                 flag=False
# # # #                 break
# # # #             else:
# # # #                 flag=True
# # # #
# # # #     if flag==True:
# # # #         print("prime number")
# # # #     else:
# # # #         print("not a prime number")
# # # #
# # # #
# # # # prime(7)
# # #
# # # #
# # # # # factorial
# # # #
# # # # def fact(n):
# # # #     if n==0 or n ==1:
# # # #         return 1
# # # #     else:
# # # #         return n*fact(n-1)
# # # #
# # # #
# # # #
# # # # print(fact(5))
# # #
# # #
# # # #
# # # # # fibonacci number 0 1 1 2 3 5 8 13 etc
# # # #
# # # #
# # # #
# # # # def feb(n):
# # # #     if n==0:
# # # #         return 0
# # # #     elif n==1 :
# # # #         return 1
# # # #     else:
# # # #         return feb(n-1)+feb(n-2)
# # # #
# # # #
# # # # print(feb(4))
# # # #
# # # # def feb1(n):
# # # #     a, b = 0, 1
# # # #     for i in range(n):
# # # #         a, b = b, a + b
# # # #     return a
# # # #
# # # # print(feb1(4))
# # #
# # #
# # # # amstrong 153 legth digit the temp =n  3**3 + 5**3 + 1**3
# # #
# # #
# # # def amstrong(number):
# # #     result=0
# # #     temp=number
# # #     length=len(str(number))
# # #     while temp >0:
# # #         digit= temp % 10
# # #         print('digit' , digit)
# # #         result += digit ** length
# # #         print(result)
# # #         temp=temp//10
# # #
# # #
# # # amstrong(153)
# # #
# # #
# # #
# # #
# # #
# # #
# #
# # arr = [10, 20, 4]
# #
# # arr2= [arr[i]  for i in range( len(arr)-1,-1,-1)]
# #
# # print(arr2)
# #
# #
# # my_list = [1, 2, 3, 4, 5]
# #
# # my_list[0],my_list[-1]=my_list[-1],my_list[0]
# #
# # print(my_list)
# #
#
#
# # # right angle
# #
# #
# # for i in range(5):
# #     for j in range(i+1):
# #         print("*",end='')
# #     print()
#
# # *
# # **
# # ***
# # ****
# # *****
#
# # inverted
# #
# # n=5
# # for i in range(n):
# #     print((n-i)* '*')
# #
# #
#
#
# # triangle  '' n- (i+1)    * (i+1)
#
# n=5
# for i in range(5):
#     print(' ' * (n-(i+1)) + ('* ')*(i+1))
#
#
#
# print()
# # reverrse
#
# n=5
# for i in range(5):
#     print(' ' * (i) + ('* ')*(n-i))
#
# print()
#
# n=5
# for i in range(n):
#     print(' ' * (n-(i+1)) + ('* ')*(i+1))
# for i in range(n-1):
#     print(' ' * ((i+1)) + ('* ')*(n-(i+1)))
#
#


# op aaaabbbbcc
# str='a4b3c2'
#
# output=''
# for char in str:
#     if char.isalpha():
#         letter=char
#     else:
#         output+=letter* int(char)
#
#
# print(output)
#
#
# Input: a4k3b2
# Outpt: aeknbd

#
# input='a4k3b2'
#
# output=''
#
# for char in input:
#     if char.isalpha():
#         previous=char
#         output+=char
#     else:
#         output = output+chr(ord(previous)+int(char))
#
#
# print(output)

#
# str='ABCABCABBCDE'
#
# d={}
#
#
# for char in str:
#     d[char]=d.get(char,0)+1
#
# print(d)
#
# for k,v in d.items():
#     print(k,v)

#
# str='one two three four five six seven'
# # Output: 'one owt three ruof five xis seven'
#
#
# l1=str.split(" ")
# print(l1)
#
# l2=[]
#
#
# for i in range(len(l1)):
#     if i%2 ==0:
#         l2.append(l1[i])
#     else:
#         l2.append(l1[i][::-1])
#
# print(l2)
#
#


# input: aaaabbbccz
# 2) output: 4a3b2c1z


input='aaaabbbcczz'
print('input>>',input)
counter=1
previous=input[0]
output=''

i=1;

while i < len(input):
    if input[i]==previous:
        counter+=1
    else:
        output =output+str(counter)+ previous
        previous=input[i]
        counter=1
    if i==len(input)-1:
        output =output+str(counter)+ previous
    i=i+1

print(output)