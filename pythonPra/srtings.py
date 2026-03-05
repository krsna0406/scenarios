# to print the string in forward and negative (reverse)
# s = "Learning Python is very easy !!!"
#
# i=0
# slen=len(s)
#
# while i<slen:
#     print(s[i],end='')
#     i=i+1
#
# #in reverse direction
#
# j=-1
#
# while j> -(slen+1):
#     print(s[j],end='')
#     j=j-1
#
#
# print()
# print(s[::-1])


# s = input("Enter Some String:")
# 2) i=len(s)-1
# 3) target=''
# 4) while i>=0:
#     5) target=target+s[i]
# 6) i=i-1
# 7) print(target)

#
#
# # reversing the words
# print('reversing the words')
# s=input("Enter Some String:")
# strarr=s.split(" ")
#
# tempstr=''
#
# for str in strarr:
#     print(str)
#     tempstr=tempstr+' '+str[::-1]
# print(tempstr)
#
#
# tempstrarr=[]
#
# for str in strarr:
#     tempstrarr.append(str[::-1])
#
# print(' '.join(tempstrarr))  #use join  to convert the list of STRINGS to a normal string
#


# Input: B4A1D3
# Output: ABD134

# s1=s2=''
#
#
# for temp in sorted('B4A1D3'):
#     if temp.isalpha():
#         s1=s1+temp
#     else:
#         s2=s2+temp
# print(s1,"   ",s2)
# print(s1+s2)



# Input: a4b3c2
# Output: aaaabbbcc


# str='a4b3c2'
# temp=''
# for s in str:
#     if s.isalpha():
#         letter=s
#         temp=temp+letter
#     else:
#         print('it is not letter ,it is number so multiply this with previous letter')
#         temp=temp+letter*int(s)
# print(temp)



# Input: a4k3b2
# Outpt: aeknbd

# s='a4k3b2'
# tempstr=''
# for str in s:
#     if str.isalpha():
#         tempstr=tempstr+str
#         letter=str
#     else:#it is number
#         tempstr=tempstr+chr(ord(str))
#         print(tempstr)



# Input: ABCDABBCDABBBCCCDDEEEF
# Output: ABCDEF

# s='ABCDABBCDABBBCCCDDEEEF'
# temp=''
#
# for str in s:
#     if str not in temp:
#         temp=temp+str
#
# print(temp)


# Input: ABCABCABBCDE
# Output: A-3,B-4,C-3,D-1,E-1

# add the items in dict and do

# string='ABCABCABBCDE'
# dct={}
# for str in string:
#     dct[str]=dct.get(str,0)+1
# print(dct)
#
#
# for k,v in dct.items():
#     print("{}-{}".format(k,v))



# reverse and reversed
#
# my_list = [1, 2, 3, 4]
# mylist2=my_list.reverse()
# print(id(my_list))
# print(id(mylist2))


# input: aaaabbbccz
# output: 4a3b2c1z


# strarr='aaaabbbccz'
# pre=strarr[0]
# cnt=0
# finalStr=''

#
# for s in strarr:
#     if s==pre:
#         cnt=cnt+1;
#     else:
#         finalStr=finalStr+str(cnt)+pre
#         pre=s
#         cnt=1
#         print(pre)
# print(finalStr)


# input: aaaabbbccz
# output: 4a3b2c1z
# IMP SUPER
# str1='aaaabbbccz'
#
# dct={}
#
# for sr in  str1:
#     print(sr)
#     dct[sr]=dct.get(sr,0)+1
# print(dct)
#
# tempStr=''
# for k,v in dct.items():
#     tempStr=tempStr+str(v)+k
#
# print(tempStr)



# s='ABCDABXXXBCDABBBBCCCZZZZCDDDDEEEEEF'
# l=[]
# for ch in s:
#     if ch not in l:
#         l.append(ch)
# for ch in sorted(l):
#     print('{} occurrs {} times'.format(ch,s.count(ch)))
#

    # output

#     A occurrs 3 times
# B occurrs 7 times
# C occurrs 6 times
# D occurrs 6 times
# E occurrs 5 times
# F occurrs 1 times
# X occurrs 3 times
# Z occurrs 4 times


s='ABCDABXXXBCDABBBBCCCZZZZCDDDDEEEEEF'

dct={}
for str in s:
    dct[str]=dct.get(str,0)+1
print(dct)


for k,v in dct.items():
    print('{}-{}'.format(k,v))


