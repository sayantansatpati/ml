
#!/usr/bin/env python

import sys
import re
import itertools

for line in sys.stdin:
    try:
        # Remove leading & trailing chars
        line = line.strip()
        # Split the line by <TAB> delimeter
        items = re.split(r'\s+', line)
        #Sort the list
        items.sort()
        
        l = len(items)
        
        for i in xrange(l):
            print '%s,*\t%d' %(items[i], 1)
            for j in xrange(i+1, l):
               # Emit both (a,b) and (b,a)
               print '%s,%s\t%d' %(items[i], items[j], 1)
               print '%s,%s\t%d' %(items[j], items[i], 1)
    except Exception as e:
        print e