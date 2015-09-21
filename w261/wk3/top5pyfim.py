__author__ = 'ssatpati'

import re
from fim import apriori

baskets = []
with open('ProductPurchaseData.txt', 'r') as f:
    for line in f:
        items = re.split(r'\s', line)
        items.sort()
        baskets.append(items)

for r in apriori(baskets, target='r', zmax=2, supp= -100, report='c', eval='c', conf=90):
    if r[0]:
	print '%s\t%s\t%s' %(r[0],r[1],r[2])
