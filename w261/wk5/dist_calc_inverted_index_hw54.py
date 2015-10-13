#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re, sys
import ast
import math
from mrjob.protocol import RawValueProtocol,JSONProtocol
from itertools import combinations

class DistanceCalcInvertedIndex(MRJob):
    
    STOP_WORDS = [u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am', u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against', u'between', u'into', u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', u'most', u'other', u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same', u'so', u'than', u'too', u'very', u's', u't', u'can', u'will', u'just', u'don', u'should', u'now']
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer
                  ),
            MRStep(mapper=self.mapper_dist,
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
        
        # Normalize & Round to 3
        if docId.lower() not in self.STOP_WORDS:
            total_cnt = sum(stripe.values())
            for word,cnt in stripe.iteritems():
                if word.lower() not in self.STOP_WORDS:
                    yield word, (docId, round(float(cnt)/total_cnt,5))
    
    def reducer(self, word, docId_count):
        docId_counts = [i for i in docId_count]
        yield word, docId_counts
        
    # Step: 2
        
    def mapper_dist(self, key, docId_counts):
        #sys.stderr.write('{0} # {1}\n'.format(key, docId_counts))
        docId_counts = sorted(docId_counts)
        l = len(docId_counts)
        if l < 7000:
            for i in xrange(l):
                for j in xrange(l):
                    if j > i:
                        x = docId_counts[i][1]
                        y = docId_counts[j][1]
                        yield (docId_counts[i][0], docId_counts[j][0]), round((docId_counts[i][1] * docId_counts[j][1]),5)
    
    
    def combiner_dist(self, docId_pair, values):
        yield docId_pair, round(sum(values),5)
        
        
    def reducer_dist(self, docId_pair, values):
        yield docId_pair, round(sum(values),5)
        
        
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