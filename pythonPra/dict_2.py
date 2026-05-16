# # # question -16
# # ## Python Program to Python program to find the sum of all items in a dictionary
# # ## Input : {‘a’: 100, ‘b’:200, ‘c’:300}
# # ## Output : 600
#
#
#
# a={'a':100, 'b':200, 'c':300}
# sum=0
#
# for v in a.values():
#     sum+=v
#
# print(sum)
#
#
#
# # # question -15
# # ## Python Program to  Remove All Duplicates from a Given String in Python
# # ## Input : Input : feeksforfeeks
# #
# # ## Output : feksfor
#
# inputstr='feeksforfeeks'
# tempstr= []
#
# for char in inputstr:
#     if char in tempstr:
#         pass
#     else:tempstr.append(char)
#
# print(tempstr)
#
#
# print(''.join(tempstr))
#
#
# # # question -14
# # ## Python Program to  Words Frequency in String
# # ## Input : test_str = 'hello is best'
# # ## Output : {'hello': 1, 'is': 1, 'best': 1}
# #
# # use dict
#
#
# print("AAAAAAAAAAAAAAAAA")
#
# test_str = 'hello is best hello'
#
# tempstrarr=test_str.split(" ")
#
#
# d1={}
#
#
# for i in tempstrarr:
#     d1[i]=d1.get(i,0)+1
#
# print(d1)
#
#
#
#
# for k,v in d1.items():
#     print( " {0} --- {1}".format(k,v))
#
#





# # question -12
# ## Python Program to Reverse Words in a Given String in Python
# ## Input : str =" geeks quiz practice code"
# ## Output : str = code practice quiz geeks

#
#
# str=" geeks quiz practice code"
#
# strarr=str.strip().split(" ")
#
# print(strarr)
# print(strarr[::-1])
#
# print(" ".join(strarr[::-1]))
#
#


# # question -11
# ## Python Program to  Check if a String is Palindrome or Not
# ## find reverse of the string and then Check if reverse and original are same or not.
# ## Input : malayalam
# ## Output : Yes
#

#
# input="malayalam"
#
#
# result=input[::-1]
#
# result1='palindrome' if input==result else 'not palidrome'
#
# print(result1)



# # question -10
# ## Python Program to Swap Two Elements in a List
# ## my_list = [1, 2, 3, 4, 5]
#


# # question -8
# ## Python Program to Array Rotation
# ## Input : arr[] = {10, 20, 4}
#
#

# # In[116]:
#
#
# arr=[1,2,3,4,5]
# print([arr[i] for i in range(len(arr)-1,-1,-1)])
#



# # question -5
# ## Armstrong number is a number that is equal to the sum of its digits,
# each raised to the power of the number of digits in the number.
# For example, 153 is an Armstrong number because 1^3 + 5^3 + 3^3 = 153.




def anumber(number):
    length=len(str(number))

    temp=number
    sum=0

    while temp>0:
        digit=temp%10
        sum +=digit**len()
        temp=temp/10




