#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations

class DistributionNgram(MRJob):
    
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
        yield int(tokens[1]), tokens[0]
    
    def reducer(self, count, ngrams):
        for ngram in ngrams:
            yield count, ngram
    
if __name__ == '__main__':
    DistributionNgram.run()