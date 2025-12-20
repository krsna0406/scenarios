# s='malayalam'
# a=''
# for i in s:
#     a=i+a
#     print(a)
# if a==s:
#     print('palindrome')
# else:
#     print('not')


s='hello is best hello'

list=s.split()
# print(list)
d={}
for i in list:
    if i in d:
        d[i]=d[i]+1
    else:
        d[i]=1;
print(d)


