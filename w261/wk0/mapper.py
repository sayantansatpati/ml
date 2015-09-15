#!/usr/bin/python
import sys
import re
count = 0
filename = sys.argv[2]
findword = sys.argv[1]
with open (filename, "r") as myfile:
    for line in myfile:
        # Case Insensitive Regex Search for the word in the line
        match = re.search("\\b" + findword + "\\b", line, re.IGNORECASE)
        if match:
            count += 1
# Print count from each mapper
print count