
#!/usr/bin/env python

import sys
import re
import itertools

for line in sys.stdin:
    try:
        # Remove leading & trailing chars
        line = line.strip()
        # Split the line by <TAB> delimeter
        items = re.split(r'\s', line)
               
        for i in items:
            print '%s\t%d\t%d' %(i, 1, len(items))
        
        ''' Commenting out
        #Sort the list
        items.sort()
        
        for c in itertools.combinations(items, 1):
            print '%s,%s\t%d\t%d' %(c[0], '*', 1, len(items))
            
        for c in itertools.combinations(items, 2):
            print '%s,%s\t%d' %(c[0], c[1], 1)
        
        for c in itertools.combinations(items, 3):
            print '%s,%s,%s\t%d' %(c[0], c[1], c[2], 1)
        '''
    except Exception as e:
        print e