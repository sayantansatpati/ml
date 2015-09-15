#!/usr/bin/python
import sys
import re
cnt = 0
wordCountDict = {}
for file in sys.argv:
    if cnt == 0:
        cnt += 1
        continue
        
    with open (file, "r") as myfile:
        for line in myfile:
            wc = re.split(r'\t+', line.strip())
            if wc[0] not in wordCountDict:
                wordCountDict[wc[0]] = int(wc[1])
            else:
                wordCountDict[wc[0]] += int(wc[1])
                
# Print count from each mapper
for k,v in wordCountDict.items():
    print "{0}\t{1}".format(k,v)