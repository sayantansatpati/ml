#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.compat import get_jobconf_value
import ast,sys


class TopNPageRanks(MRJob):
    
    INPUT_PROTOCOL = RawProtocol
    
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_topN_init,
                   mapper=self.mapper_topN,
                   mapper_final=self.mapper_topN_final,
                   reducer_init=self.reducer_topN_init,
                   reducer=self.reducer_topN,
                   reducer_final=self.reducer_topN_final,
                   jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1rn',
                            'mapred.reduce.tasks': 1
                            }
                  )
        ]
    
    def mapper_topN_init(self):
        self.TOP_N = 100
        self.top_N_pairs = []
    
    def mapper_topN(self, key, value):
        key = key.replace("\"","")
        key = key.replace("\\","")
        adj_list = ast.literal_eval(value)
        
        self.top_N_pairs.append((adj_list['score'], key))
        if len(self.top_N_pairs) > self.TOP_N:
            self.top_N_pairs.sort(key=lambda x: -x[0])
            self.top_N_pairs = self.top_N_pairs[:self.TOP_N]
            
    def mapper_topN_final(self):
        sys.stderr.write('##### [Mapper_Final]: {0}\n'.format(len(self.top_N_pairs)))
        for e in self.top_N_pairs:
            yield e[0], e[1]
        
    def reducer_topN_init(self):
        self.TOP_N = 100
        self.top_N_pairs = []
            
    def reducer_topN(self, key, values):
        for value in values:
            self.top_N_pairs.append((key, value))
        if len(self.top_N_pairs) > self.TOP_N:
            self.top_N_pairs.sort(key=lambda x: -x[0])
            self.top_N_pairs = self.top_N_pairs[:self.TOP_N]
        
    def reducer_topN_final(self):
        sys.stderr.write('##### [Reducer_Final]: {0}\n'.format(len(self.top_N_pairs)))
        for e in self.top_N_pairs:
            yield e[0], e[1]
    
if __name__ == '__main__':
    TopNPageRanks.run()