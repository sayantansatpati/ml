#!/usr/bin/python
import sys
sum = 0
for line in sys.stdin:
    # Sum the counts across all mappers
    sum += int(line)
# Final Sum
print sum