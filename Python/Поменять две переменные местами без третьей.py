a =int(input("a =  "))
b =int(input("b =  "))
(a,b) = (b,a)

print('a= ',a,'b= ',b)

#или так
(a,b) = (b,a)

a = a+b
b = a- b
a= a-b

print('a= ',a,'b= ',b)
