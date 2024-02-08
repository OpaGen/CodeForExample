def SumNumber():
    x = int(input())
    if x == 0:
        return 0
    else:
        return x + SumNumber()

print(SumNumber())
