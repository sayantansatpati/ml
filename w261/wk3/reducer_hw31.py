#!/usr/bin/python
import sys
import re
import heapq
from sets import Set

'''
a1,* 1 4
a1,* 1 5
a1,b1 1
a1,b1 1
a1,b2 1
a1,b2 1
a2,* 1 6
'''

itemset_1_cnt = 0
itemset_2_cnt = 0

itemset_1_last = None
itemset_2_last = None

THRESHOLD = 100

# Statistics
# Unique Items
uniq = Set()
# Max Basket Length
max_basket_len = 0
# Total Itemset Counts for Sizes: 1 & 2
total_itemset_1 = 0
total_itemset_2 = 0

d_counts = {}

def update_counts():
    global itemset_1_last
    global d_counts
    global total_itemset_1
    global total_itemset_2
    key = '{0},{1}'.format(itemset_1_last, '*')
    if d_counts.get(key, 0) >= THRESHOLD:
        total_itemset_1 += 1
        for k,v in d_counts.iteritems():
            if k != key:
                if v >= THRESHOLD:
                    total_itemset_2 += 1

for line in sys.stdin:
    # Remove leading & trailing chars
        line = line.strip()
        # Split the line by <TAB> delimeter
        tokens = re.split(r'\s+', line)
    
        # Split the key by <COMMA> delimeter
        items = tokens[0].split(",")
        i1 = items[0]
        i2 = items[1]
        
        # Count
        count = int(tokens[1])
        
        if not itemset_1_last:
            itemset_1_last = i1
        
        if itemset_1_last != i1:
            # Emit Contents of Dict
            update_counts()
            
            if i2 == '*':
                uniq.add(i1)
                basket_len = int(tokens[2])
                if basket_len > max_basket_len:
                    max_basket_len = basket_len
                            
            d_counts.clear()
            itemset_1_last = i1
        else:
            key = tokens[0]
            d_counts[key] = d_counts.get(key, 0) + count
            
            if i2 == '*':
                uniq.add(i1)
                basket_len = int(tokens[2])
                if basket_len > max_basket_len:
                    max_basket_len = basket_len
                    
# Last Record
update_counts()
                    
print '=== Statistics ==='
print 'Total Unique Items: %d' %(len(uniq))
print 'Maximum Basket Length: %d' %(max_basket_len)
print 'Total # itemsets of size 1: %d' %(total_itemset_1)
print 'Total # itemsets of size 2: %d' %(total_itemset_2)
        
        