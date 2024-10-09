x = int(input())
t = 1
s1 = '+___ ' * x
s2 = ''
s3 = '|__\ ' * x
s4 = '|    ' * x

for i in range(1, x + t, t):
    s2 = s2 + '|' + str(i) + ' / '

print(s1)
print(s2)
print(s3)
print(s4)
