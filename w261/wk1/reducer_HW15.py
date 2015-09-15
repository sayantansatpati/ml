#!/usr/bin/python
import sys
import re
import math

# Totals
vocab = 0
vocab_spam = 0
vocab_ham = 0

vocab = {}
word_counts = {
    "1": {},
    "0": {}
}

num_spam = 0
num_ham = 0

cnt = 0
# Calculate the totals in Reducer First Pass
for file in sys.argv:
    if cnt == 0:
        cnt += 1
        continue
        
    with open (file, "r") as myfile:
        last_line_num = -1
        last_spam = -1
        
        for line in myfile:
            tokens = re.split(r'\t+', line.strip())
            line_num = int(tokens[0])
            spam = int(tokens[1])
            word = tokens[2]
            count = float(tokens[3])
            
            # Init
            if last_line_num == -1:
                last_line_num = line_num
                last_spam = spam
            
            # Add Vocab per line
            if word not in vocab:
                vocab[word] = 0.0
            if word not in word_counts[str(spam)]:
                word_counts[str(spam)][word] = 0.0
            vocab[word] += count
            word_counts[str(spam)][word] += count
                    
            if last_line_num != line_num:
                if last_spam == 1:
                    num_spam += 1
                else:
                    num_ham += 1
                
            last_line_num = line_num
            last_spam = spam
            
        # Last Line
        if last_spam == 1:
            num_spam += 1
        else:
            num_ham += 1
                
# At the end of first pass
print 'Num Spam: {0}, Num Ham: {1}'.format(num_spam, num_ham)
print '''Total Vocab: {0},
       Total Unique Vocab: {1},
       Total Spam Vocab: {2}, 
       Total Ham Vocab: {3}'''.format(sum(vocab.values()), 
                                    len(vocab),
                                    sum(word_counts['1'].values()), 
                                    sum(word_counts['0'].values())
                                   )
                                    

prior_spam = (num_spam * 1.0) / (num_spam + num_ham)
prior_ham = (num_ham * 1.0) / (num_spam + num_ham)
print '[Priors] Spam: {0}, Ham: {1}'.format(prior_spam, prior_ham)

spam_likelihood_denom = sum(word_counts['1'].values()) + len(vocab)
ham_likelihood_denom = sum(word_counts['0'].values()) + len(vocab)

# Calculate the Conditionals/Likelihood in Next Pass
reducer_output_list = []
cnt = 0
for file in sys.argv:
    if cnt == 0:
        cnt += 1
        continue
        
    with open (file, "r") as myfile:
        last_line_num = -1
        log_prob_spam = 0
        log_prob_ham = 0
        
        for line in myfile:
            
            tokens = re.split(r'\t+', line.strip())
            line_num = int(tokens[0])
            spam = int(tokens[1])
            word = tokens[2]
            count = int(tokens[3])
            
            # Init
            if last_line_num == -1:
                last_line_num = line_num
            
            if last_line_num != line_num:
                # Calculate the Naive Bayes Scores for Document Classification
                spam_score = log_prob_spam + math.log(prior_spam)
                ham_score = log_prob_ham + math.log(prior_ham)
                reducer_output_list.append((spam, spam_score, ham_score))
                # Reset log prob
                log_prob_spam = 0
                log_prob_ham = 0
            else:
                # Calcuate the log likelihoods Using Laplace Smoothing
                spam_likelihood = (word_counts['1'].get(word, 0.0) + 1) / spam_likelihood_denom
                ham_likelihood = (word_counts['0'].get(word, 0.0) + 1) / ham_likelihood_denom
                log_prob_spam += math.log( spam_likelihood )
                log_prob_ham += math.log( ham_likelihood )
            
            last_line_num = line_num
            
        # Last Line
        spam_score = log_prob_spam + math.log(prior_spam)
        ham_score = log_prob_ham + math.log(prior_ham)
        reducer_output_list.append((spam, spam_score, ham_score))
        
total = 0.0
miscat = 0.0
for (spam, spam_score, ham_score) in reducer_output_list:
        total += 1.0
        pred_class = 'HAM'
        if spam_score > ham_score:
            pred_class = 'SPAM'
        if (spam == 1 and pred_class == 'HAM') or (spam == 0 and pred_class == 'SPAM'):
            miscat += 1.0
            
        print "{0}\t{1}\t{2}\t{3}".format(spam, spam_score, ham_score, pred_class)

error = miscat * 100 / total
print "Accuracy: {0}, Error Rate: {1}, # of Miscats: {2}".format((100 - error), error, miscat)