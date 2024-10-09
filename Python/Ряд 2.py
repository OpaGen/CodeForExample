a, b = int(input()), int(input())

t = 1
if a > b:
    t = -1

for i in range(a, b + t, t):
    print(i)
