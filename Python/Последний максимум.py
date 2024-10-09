sp = input().split()
mn = int(sp[0])
mi = 0

for i in range(0, len(sp), 1):
    if int(sp[i]) >= mn:
        mn = int(sp[i])
        mi = i

print(mn, mi)
