mas = input().split()
mx = int(mas[0])
mn = int(mas[0])
mxi = 0
mni = 0

for i in range(0, len(mas)):
    if int(mas[i]) >= mx:
        mx = int(mas[i])
        mxi = i
    if int(mas[i]) <= mn:
        mn = int(mas[i])
        mni = i

mas[mxi] = mn
mas[mni] = mx

print(' '.join(map(str, mas)))
