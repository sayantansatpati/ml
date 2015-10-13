#!/usr/bin/python2.7
import nltk
from nltk.corpus import wordnet as wn
from itertools import combinations
import sys, re

def synonyms(string):
    syndict = {}
    for i,j in enumerate(wn.synsets(string)):
        syns = j.lemma_names()
        for syn in syns:
            syndict.setdefault(syn,1)
    return syndict.keys()

wordList = []
f = open('top10kWords.txt','r')
for word in f:
    word = word.strip()
    wordList.append(word)
f.close()
    
pairs = {}
for word in wordList:
    syns = synonyms(word)
    keepSyns = []
    for syn in syns:
        syn = str(re.sub("_"," ",syn))
        if syn != word:
            if syn in wordList:
                keepSyns.append(syn)
    for syn in keepSyns:
        pair = ",".join(sorted([word,syn]))
        pairs[pair] = 1

f = open('synonyms.txt','w')
for pair in sorted(pairs.keys()):
    f.writelines(pair+"\n")
f.close()