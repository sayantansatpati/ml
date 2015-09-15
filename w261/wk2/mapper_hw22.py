
#!/usr/bin/env python

import sys
import re

def strip_special_chars(word):
    return re.sub('[^A-Za-z0-9]+', '', word)

for line in sys.stdin:
    try:
        # Remove leading & trailing chars
        line = line.strip()
        # Split the line by <TAB> delimeter
        email = re.split(r'\t+', line)

        # Check whether Content is present
        if len(email) < 4:
            continue

        # Get the content as a list of words
        content = email[len(email) - 1].split()

        for w in content:
            w = strip_special_chars(w)
            if w == 'assistance':
                print '%s\t%d' % (w, 1)
    except Exception as e:
        print line
        print e