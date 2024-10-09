sp = input().split()
c = 0

for i in range(0, len(sp), 1):
    if int(sp[i]) > 0:
        c += 1

print(c)
