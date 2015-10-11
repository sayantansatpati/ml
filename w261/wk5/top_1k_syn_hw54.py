#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import ast
import sys
from itertools import combinations

class Top1KSynonyms(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer,
                   jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1rn',
                            'mapred.reduce.tasks': 1
                            }
                   )
        ]
    
    def mapper(self, _, line):
        tokens = line.strip().split('\t')
        [w1,w2] = ast.literal_eval(tokens[1])
        if w1 != w2:
            yield float(tokens[0]),tokens[1]
    
    def reducer(self, key, values):
        for value in values:
            yield key, value
    
if __name__ == '__main__':
    Top1KSynonyms.run()