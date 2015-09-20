__author__ = 'ssatpati'


__author__ = 'ssatpati'

import re
from fim import apriori

baskets = []
with open('ProductPurchaseData.txt', 'r') as f:
    for line in f:
        items = re.split(r'\s', line)
        items.sort()
        baskets.append(items)

for r in apriori(baskets, target='r', supp= -100):
    print r