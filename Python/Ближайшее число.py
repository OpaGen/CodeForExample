import math
n = int(input())
mas = input().split()
x = int(input())
xa = 10000

for i in range(0, len(mas)):
    if math.sqrt((int(mas[i]) - x) ** 2) <= xa:
        xa = math.sqrt((int(mas[i]) - x) ** 2)
        n = int(mas[i])
print(n)
