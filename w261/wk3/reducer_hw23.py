
#!/usr/bin/env python

import sys
import os
import re
import math

# Totals from Mapper
total = 0
total_spam = 0
total_ham = 0

vocab = 0
vocab_spam = 0
vocab_ham = 0
word_count = {}

word_list = os.environ['WORDS'].split(",")

for line in sys.stdin:
    try:
         # Remove leading & trailing chars
        line = line.strip()
        # Split the line by <TAB> delimeter
        tokens = re.split(r'\t+', line)
        
        if tokens[0] == 'TOTAL_DOCUMENTS':
            total += float(tokens[1])
            total_spam += float(tokens[2])
            total_ham += float(tokens[3])
        elif tokens[0] == 'TOTAL_WORDS':
            vocab = float(tokens[1])
            vocab_spam = float(tokens[2])
            vocab_ham = float(tokens[3])
        else:
            word_count[tokens[0]] = (float(tokens[1]), float(tokens[2]))
    except Exception as e:
        sys.exit(1)

prior_spam = total_spam / total
prior_ham = total_ham / total

spam_lhood_denom = vocab_spam + vocab
ham_lhood_denom = vocab_ham + vocab
spam_lhood_log = 0.0
ham_lhood_log = 0.0
for w in word_list:
    spam_lhood_log += math.log( (word_count[w][0] + 1.0) / spam_lhood_denom )
    ham_lhood_log += math.log( (word_count[w][1] + 1.0) / ham_lhood_denom )
spam_score = spam_lhood_log + math.log(prior_spam)
ham_score = ham_lhood_log + math.log(prior_ham)

classification = 'HAM'
if spam_score > ham_score:
    classification = 'SPAM'
   
print '#<Feature>\t<Spam_Score>\t<Ham_Score>\t<Predicted_Class>'
print '%s\t%d\t%d\t%s' %(",".join(word_list), spam_score, ham_score, classification)