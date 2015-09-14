#!/usr/bin/python
import sys
for line in sys.stdin:
    tokens = line.strip().split(",")
    print "%s" %(tokens[0])