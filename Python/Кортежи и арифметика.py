a = int(input("a= "))
b = int(input("b= "))
a,b = b, a
print(a, b)

a = a + b
b = a - b
a = a - b

print(a , b)


a = tuple('hello')
print(a)
