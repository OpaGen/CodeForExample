sp = input().split()

for i in range(0, len(sp), 1):
    if int(sp[i]) % 2 == 0:
        print(sp[i], end=' ')
