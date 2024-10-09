s = input()
sb = 'h'

pstart = s.find(sb)
pend = s.rfind(sb)

snew = s[:pstart] + s[pend + 1:]

print(snew)
