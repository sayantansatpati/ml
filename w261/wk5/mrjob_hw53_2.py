#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations

class FrequentUnigrams(MRJob):
    '''
    def jobconf(self):
        orig_jobconf = super(FrequentUnigrams, self).jobconf()        
        custom_jobconf = {
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1,1rn',
        }
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        self.jobconf = combined_jobconf
        return combined_jobconf
    '''
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
             MRStep(mapper=self.mapper_frequent_unigrams,
                   reducer=self.reducer_frequent_unigrams,
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
            yield unigram, int(tokens[1])
    
    def combiner(self, unigram, counts):
        yield unigram, sum(counts)
    
    def reducer(self, unigram, counts):
        yield unigram, sum(counts)
        
    def mapper_frequent_unigrams(self, unigram, count):
        yield count, unigram
        
    def reducer_frequent_unigrams(self, count, unigrams):
        for unigram in unigrams:
            yield count, unigram
    
if __name__ == '__main__':
    FrequentUnigrams.run()