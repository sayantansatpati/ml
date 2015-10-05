#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations

class DenseWords(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer,
                   ),
            MRStep(mapper=self.mapper_max_min,
                   reducer=self.reducer_max_min,
                   jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1rn',
                            }
                   )
        ]
    
    def mapper(self, _, line):
        tokens = line.strip().split('\t')
        unigrams = tokens[0].split()
        density = round((int(tokens[1]) * 1.0 / int(tokens[2])), 3)
        for unigram in unigrams:
            yield unigram, density
            
    def combiner(self, unigram, densities):
        densities = [d for d in densities]
        yield unigram, min(densities) 
        yield unigram, max(densities)
        
    def reducer(self, unigram, densities):
        densities = [d for d in densities]
        yield unigram, min(densities)
        yield unigram, max(densities)
        
    def mapper_max_min(self, unigram, density):
        yield density, unigram
        
    def reducer_max_min(self, density, unigrams):
        for unigram in unigrams:
            yield density, unigram
    
if __name__ == '__main__':
    DenseWords.run()