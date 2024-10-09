sp = input().split()
m = 1001

for i in range(1, len(sp), 1):
    if int(sp[i]) > 0 and int(sp[i]) < m:
        m = int(sp[i])

print(m)
