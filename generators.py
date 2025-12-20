# g=(x*x for x in range(100))
#
# print(g)
#
# for x in g:
#     print(x)
################################################


# def genFun():
#     yield 0
#     yield 1
#     yield 2
#
# g=genFun()
# for x in g:
#     print(x)


## fibonacci


def feb_fun():
    a, b = 0, 1
    while True:
        yield a
        a, b = b, a + b

for x in feb_fun():
    if x > 100:
        break
    print(x,end=',')

#nth fibonacci


print(end='\n')

limit=1
for x in feb_fun():
    if limit<=10:
        print(x,end=' ')
        limit+=1
    else:
        break




