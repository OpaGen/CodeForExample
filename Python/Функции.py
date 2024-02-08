#Прога вычисления дискременанта с помошью функций и коротких функций
def discremenant(a,b, c = 0):
        return (b**2) - (4*a*c)

xOne = lambda a,b,d : (-b-(d**0.5))/(2*a)
xTwo = lambda a,b,d : (-b+(d**0.5))/(2*a)

def countkorney(d,x1,x2):
        if d > 0:
            return print(d,x1,x2)
        elif d==0:
            return print(d,x1)
        else:
            return print(d,0)

a = int(input("a= "))
b = int(input("b= "))
c = int(input("c= "))

d = discremenant(a,b,c)
x1 = xOne(a,b,d)
x2 = xTwo(a,b,d)

countkorney(d,x1,x2)

