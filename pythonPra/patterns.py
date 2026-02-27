import calendar

#
# n=5
#
# for i in range(n):
#     for j in range(i+1):
#         print("*",end=" ")
#     print()
#
# for i in range(n):
#     print("* "* (n-i))
#
#
# print("pyramid")
# for i in range(n):
#     print(" "*(n- (i+1)), " *"*(i+1))
#
#
#     # i n-i
#     # i+1 n-i-1

#
# import calendar
#
# def last_day_of_month(year, month):
#     return calendar.monthrange(year, month)[1]
#
# # Example
# print(last_day_of_month(2026, 2))   # 28
# print(last_day_of_month(2024, 2))   # 29 (leap year)
#
# print(calendar.month(2026,2))
# print(calendar.month(2024,2))

#
# s1='abcdefg'
# s2='xyz'
# s3='12345'
# i=j=k=0
# output=''
# while i<len(s1) or j<len(s2) or k<len(s3):
#
#     if i<len(s1):
#         output=output+s1[i]
#         i=i+1
#     if j<len(s2):
#         output=output+s2[j]
#         j=j+1
#     if k<len(s3):
#         output=output+s3[k]
#         k=k+1
#
# print(output)
#
# for i in output:
#     print(i)

s='ABAABBCA'

#
# d={}
# outputstr=''
# for char in s:
#     d[char]=d.get(char,0)+1
#
# print(d)
#
# for k,v in d.items():
#     print(k,v)
#     outputstr=outputstr+k+str(v)
#
# print(outputstr)

# s='a4k3b2' # aeknbd
#
# output=''
#
# i=0
# char=''
#
# while i<len(s):
#     if s[i].isalpha():
#         print("is alpha")
#         output=output+s[i]
#         char=s[i]
#     else:
#         output=output+chr( ord(char)+int(s[i]) )
#     i=i+1
#
#
# print(output)


#
# s='aaaabbbccz' # 4a3b2c1z
#
# i=0
#
# d={}
# while i<len(s):
#     key=s[i]
#     d[key]=d.get(key,0)+1
#     i+=1
#
# tempstr=''
#
# for k,v in d.items():
#     print(k,v)
#     tempstr=tempstr+str(v)+k
#
# print(tempstr)
#
# print(d)
#
#
# a3z2b4 #o/p aaabbbbzz

#
# s='a3z2b4'
#
#
# i=0
# output=''
#
# while i< len(s):
#     if s[i].isalpha():
#         temp=s[i]
#     else:
#         output=output+temp*(int(s[i]))
#     i+=1
#
#
#
# print(''.join(sorted(output)))





