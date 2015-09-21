#!/usr/bin/python
import sys
import re
from sets import Set

'''
a1 1 5
a1 1 5
a1 1 6
a2 1 18
'''

itemset_1_cnt = 0
item_last = None

THRESHOLD = 100

# Statistics
# Unique Items
uniq = Set()
# Max Basket Length
max_basket_len = 0
# Total Itemset Counts for Sizes: 1
total_itemset_1 = 0

for line in sys.stdin:
    # Remove leading & trailing chars
        line = line.strip()
        # Split the line by <TAB> delimeter
        tokens = re.split(r'\s', line)
    
        item = tokens[0]
        cnt = int(tokens[1])
        basket_len = int(tokens[2])
        
        if not item_last:
            item_last = item
            
        # Basket Length
        if basket_len > max_basket_len:
            max_basket_len = basket_len
        
        # Unique Items
        uniq.add(item)
        
        if item_last != item:
            # Check whether itemset 1 exceeds the support of 100
            if itemset_1_cnt >= THRESHOLD:
                total_itemset_1 += 1
    
            item_last = item
            itemset_1_cnt = cnt
        else:
            itemset_1_cnt += cnt
                    
# Last Record
if itemset_1_cnt >= THRESHOLD:
    total_itemset_1 += 1

print '=== Statistics ==='
print 'Total Unique Items: %d' %(len(uniq))
print 'Maximum Basket Length: %d' %(max_basket_len)
print 'Total # frequent itemsets of size 1: %d' %(total_itemset_1)
        
        