
# if no is divisible by 1 and by no itself then it is prime number


def prime(number):
    falg=False
    if number == 0 or number ==1:
        print("not prime number")
    else:
        print("check the number here")
        for i in range(2,number):
            if number%i==0:
                flag=False
                break
            else:
                flag=True
    if flag==True:
        print("prime")
    else:
        print("not prime")

prime(7)






