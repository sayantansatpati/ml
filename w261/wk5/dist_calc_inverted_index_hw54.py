#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, sys
import ast
import math
from mrjob.protocol import RawValueProtocol,JSONProtocol
from itertools import combinations

class DistanceCalcInvertedIndex(MRJob):
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer
                  ),
            MRStep(mapper=self.mapper_dist,
                   combiner=self.combiner_dist,
                   reducer=self.reducer_dist
                  ),
            MRStep(mapper_init=self.mapper_top1k_init,
                   mapper=self.mapper_top1k,
                   mapper_final=self.mapper_top1k_final,
                   reducer_init=self.reducer_top1k_init,
                   reducer=self.reducer_top1k,
                   reducer_final=self.reducer_top1k_final,
                   jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1rn',
                            'mapred.reduce.tasks': 1
                            }
                  )
        ]

    # Step:1
    def mapper(self, _, line):
        tokens = line.strip().split('\t')
        
        docId = tokens[0].replace("\"","")
        stripe = ast.literal_eval(tokens[1])
        
        for word,cnt in stripe.iteritems():
            yield word, (docId, int(cnt))
    
    def reducer(self, word, docId_count):
        docId_counts = [i for i in docId_count]
        yield word, docId_counts
        
    # Step: 2
        
    def mapper_dist(self, key, docId_counts):
        #sys.stderr.write('{0} # {1}\n'.format(key, docId_counts))
        docId_counts = sorted(docId_counts)
        l = len(docId_counts)
        for i in xrange(l):
            for j in xrange(l):
                if j > i:
                    x = docId_counts[i][1]
                    y = docId_counts[j][1]
                    yield (docId_counts[i][0], docId_counts[j][0]), (x*x, y*y, x*y)
                    
    
    def combiner_dist(self, docId_pair, values):
        dot_x_y = 0
        x_squared = 0
        y_squared = 0
        for v in values:
            x_squared += v[0]
            y_squared += v[1]
            dot_x_y += v[2]
        
        yield docId_pair, (x_squared, y_squared, dot_x_y) 
        
        
    def reducer_dist(self, docId_pair, values):
        dot_x_y = 0
        norm_x = 0
        norm_y = 0
        for v in values:
            norm_x += v[0]
            norm_y += v[1]
            dot_x_y += v[2]
        
        yield docId_pair, float(dot_x_y) / (math.sqrt(norm_x) * math.sqrt(norm_y)) 
        
        
    # Step: 3
    
    def mapper_top1k_init(self):
        self.TOP_N = 1000
        self.top_1k_pairs = []

    def mapper_top1k(self, docId_pair, distance):
        self.top_1k_pairs.append((distance, docId_pair))
        if len(self.top_1k_pairs) > self.TOP_N:
            self.top_1k_pairs.sort(key=lambda x: -x[0])
            self.top_1k_pairs = self.top_1k_pairs[:self.TOP_N]
            
    def mapper_top1k_final(self):
        sys.stderr.write('##### [Mapper_Final]: {0}\n'.format(len(self.top_1k_pairs)))
        for e in self.top_1k_pairs:
            yield e[0], e[1]
            
    def reducer_top1k_init(self):
        self.TOP_N = 1000
        self.top_1k_pairs = []
            
    def reducer_top1k(self, distance, docId_pairs):
        for docId_pair in docId_pairs:
            self.top_1k_pairs.append((distance, docId_pair))
        if len(self.top_1k_pairs) > self.TOP_N:
            self.top_1k_pairs.sort(key=lambda x: -x[0])
            self.top_1k_pairs = self.top_1k_pairs[:self.TOP_N]
        
    def reducer_top1k_final(self):
        sys.stderr.write('##### [Reducer_Final]: {0}\n'.format(len(self.top_1k_pairs)))
        for e in self.top_1k_pairs:
            yield e[0], e[1]
        
    
if __name__ == '__main__':
    DistanceCalcInvertedIndex.run()