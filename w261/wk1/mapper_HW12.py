#!/usr/bin/python
import sys
import re

def strip_special_chars(word):
    return re.sub('[^A-Za-z0-9]+', '', word)

count = 0
filename = sys.argv[1]
wordList = sys.argv[2]
wordList = wordList.split()
wordCountDict = {}
with open (filename, "r") as myfile:
    for line in myfile:
        # Split the line by <TAB> delimeter
        email = re.split(r'\t+', line)
        
        # Check whether Content is present
        if len(email) < 4:
            continue
        
        # Get the content as a list of words
        content = email[len(email) - 1].split()
        
        if len(wordList) == 1 and wordList[0] == '*':
            for w in content:
                w = strip_special_chars(w)
                if w not in wordCountDict:
                    wordCountDict[w] = 1
                else:
                    wordCountDict[w] += 1
        else:
            for w in content:
                w = strip_special_chars(w)
                # Check if word is in word list passed to mapper
                if w in wordList:
                    if w not in wordCountDict:
                        wordCountDict[w] = 1
                    else:
                        wordCountDict[w] += 1
       
# Print count from each mapper
for k,v in wordCountDict.items():
    print "{0}\t{1}".format(k,v)