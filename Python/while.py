x = int(input())
i = 2

while i <= x:
    if x % i == 0:
        print(i)
        break
    else:
        i += 1
