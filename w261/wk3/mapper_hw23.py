
#!/usr/bin/env python

import sys
import os
import re

# Output from mapp
vocab = set()
word_counts = {
    "1": {},
    "0": {}
}
total = 0
total_spam = 0
total_ham = 0

word_list = os.environ['WORDS'].split(",")

def strip_special_chars(word):
    word = word.strip().lower()
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
        spam = email[1]
        content = email[len(email) - 1].split()
        
        # Totals
        total += 1
        if spam == '1':
            total_spam += 1
        else:
            total_ham += 1

        for w in content:
            w = strip_special_chars(w)
            
            # Add to category dict
            word_counts[spam][w] = word_counts[spam].get(w, 0.0) + 1.0
                
            # Vocab Unique
            vocab.add(w)
    except Exception as e:
        print line
        print e
        
print 'TOTAL_DOCUMENTS\t%d\t%d\t%d' % (total,total_spam,total_ham)
print 'TOTAL_WORDS\t%d\t%d\t%d' % (len(vocab), sum(word_counts['1'].values()), sum(word_counts['0'].values()))
for w in word_list:
    print '%s\t%d\t%d' %(w, word_counts['1'].get(w, 0.0), word_counts['0'].get(w, 0.0))