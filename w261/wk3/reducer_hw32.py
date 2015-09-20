#!/usr/bin/python
import sys
import re
import heapq

itemset_1_cnt = 0
itemset_2_cnt = 0

itemset_1_last = None
itemset_2_last = None

'''
a1,* 1
a1,* 1
a1,b1 1
a1,b1 1
a1,b2 1
a1,b2 1
a2,* 1
'''

THRESHOLD = 100
# Store Itemsets 2
dict = {}

for line in sys.stdin:
    # Remove leading & trailing chars
        line = line.strip()
        # Split the line by <TAB> delimeter
        tokens = re.split(r'\s+', line)
    
        # Split the key by <COMMA> delimeter
        items = tokens[0].split(",")
        i1 = items[0]
        i2 = items[1]
        
        if not itemset_1_last:
            itemset_1_last = i1
        
        if itemset_1_last != i1:
            '''
            if itemset_1_cnt >= THRESHOLD:
                confidence = (itemset_2_cnt * 1.0) / itemset_1_cnt
                print '[%d,%d]%s\t%f' %(itemset_1_cnt, itemset_2_cnt, tokens[0], confidence)
                dict[tokens[0]] = confidence
            '''
                        
            # Reset
            itemset_1_last = i1
            itemset_1_cnt = int(tokens[1])
            itemset_2_last = None
            itemset_2_cnt = 0
        else:
            if i2 == '*':
                itemset_1_cnt += int(tokens[1])
            else:
                if itemset_2_last != tokens[0]:
                    if itemset_1_cnt >= THRESHOLD and itemset_2_cnt >= THRESHOLD:
                        confidence = (itemset_2_cnt * 1.0) / itemset_1_cnt
                        #print '[%d,%d]%s\t%f' %(itemset_1_cnt, itemset_2_cnt, itemset_2_last, confidence)
                        dict[itemset_2_last] = confidence
                    itemset_2_last = tokens[0]
                    itemset_2_cnt = int(tokens[1]) 
                else:
                    itemset_2_cnt += int(tokens[1])                    

# Last Set of Counts
if itemset_1_cnt >= THRESHOLD and itemset_2_cnt >= THRESHOLD:
    confidence = (itemset_2_cnt * 1.0) / itemset_1_cnt
    #print '[%d,%d]%s\t%f' %(itemset_1_cnt, itemset_2_cnt, itemset_2_last, confidence)
    dict[itemset_2_last] = confidence

print '=== Top 5 Confidence ==='
sorted_dict = sorted(dict.items(), key=lambda x:(-x[1], x[0]))
for j,k in sorted_dict[:5]:
    print '%s\t%f' %(j,k)
        
        
        