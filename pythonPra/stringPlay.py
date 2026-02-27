# s='krishna'

# print("reverse string",s[::-1])
#
# s1=reversed(s)
#
# print("__reversed__","".join(s1))

# temp=""
# for i in range(len(s)-1,-1,-1):
#     print(i,s[i])
#     temp=temp+s[i]
#     print(temp)

# by using while


# str='KRISHNA' # 6
#
# tempstr=''
# i=len(str)-1 #
# while i>=0:
#     print(str[i],end='')
#     i=i-1



# s='Learning Python Is Very Easy'
#
# s1=s.split(" ")
# print(' '.join(s1[::-1]))

# reversing the internal content

input='Durga Software Solutions'

str=input.split(" ")

print(str)
temp=[]
for i in str:
    temp.append(i[::-1])

print("".join(temp))


