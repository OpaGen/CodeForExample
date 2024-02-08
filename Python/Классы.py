class A:
        arg = 'Python'
        def g(self):
            return 'Hi'
        pass

a = A()
b = A()
a.arg = 1
b.arg = 2

c = A()
d = A()
print(a.arg , b.arg , c.g(), d.arg)
