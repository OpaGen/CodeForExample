sp = input().split()

for i in range(1, len(sp), 1):
    if int(sp[i]) > int(sp[i - 1]):
        print(sp[i], end=' ')
