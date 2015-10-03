#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations

class DenseWords(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer,
                   jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1rn',
                            }
                   )
        ]
    
    def mapper(self, _, line):
        line.strip()
        tokens = re.split("\t",line)
        unigrams = tokens[0].split()
        for unigram in unigrams:
            yield (int(tokens[1]) * 1.0 / int(tokens[2])), unigram
    
    def reducer(self, density, unigrams):
        for unigram in unigrams:
            yield unigram, density
    
if __name__ == '__main__':
    DenseWords.run()