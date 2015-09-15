#!/usr/bin/python
import sys
import re

def strip_special_chars(word):
    word = word.strip()
    
    if not word or word == '':
        return None
    
    word = re.sub('[^A-Za-z0-9]+', '', word)
    return word.lower()

count = 0
filename = sys.argv[1]
wordList = sys.argv[2]
wordList = wordList.split()

# (Line#, Spam/Ham, Dict of Word|Count)
mapper_output_list = []
line_num = 0
with open (filename, "r") as myfile:
    for line in myfile:
        # Split the line by <TAB> delimeter
        email = re.split(r'\t+', line)
        
        # Check whether Content is present
        if len(email) < 4:
            continue
            
        line_num += 1
        
        # Get the content as a list of words
        content = email[len(email) - 1].split()
        
        wordCountDict = {}
        for w in content:
            w = strip_special_chars(w)
            
            if not w:
                continue
                
            wordCountDict[w] = wordCountDict.get(w, 0) + 1
                
        mapper_output_list.append((line_num, email[1], wordCountDict))
       
# Print output from each mapper
for (line_num, spam, wordCountDict) in mapper_output_list:
    for word,count in wordCountDict.items():
        print "{0}\t{1}\t{2}\t{3}".format(line_num, spam, word, count)
    