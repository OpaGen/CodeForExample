s = str(input())
sb = ' '

p = s.find(sb)
snew = s[p + 1:] + sb + s[:p]

print(snew)
