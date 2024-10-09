x = int(input())
t = 1

for i in range(1, x + t, t):
    s = ''
    for j in range(1, i + t, t):
        s = s + str(j)
    print(s)
