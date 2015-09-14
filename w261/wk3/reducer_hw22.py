
#!/usr/bin/env python

import sys
import re

word = None
count = 0

for line in sys.stdin:
    # Remove leading & trailing chars
    line = line.strip()
    # Split the line by <TAB> delimeter
    wc = re.split(r'\t+', line)
    
    word = wc[0]
    count += int(wc[1])
    
print '%s\t%d' % (word, count)