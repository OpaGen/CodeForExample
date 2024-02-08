a = int(input("Число = "))
x = int(input("Степень = "))

if x == 0:
    print("Результат = ", 1)
elif x == 1:
    print("Результат = ",a)
else:    
    i = 1
    r = 1
    while i<=x:
        r= r * a
        i = i + 1
    print("Результат через While = ", r)    

    i = 1
    r = 1
    for i in range(0,x):
        r = r * a
    print("Результат через For = ", r)    

