from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import sys
import ast

class Graph_EDA(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                  reducer_init=self.reducer_init,
                  reducer=self.reducer,
                 reducer_final=self.reducer_final,
                  jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1n',
                            }
                   )
        ]
    

    def mapper(self, _, line):
        line = line.replace("\"","")
        # Passed only to first iteration
        t = line.strip().split('\t')
        node = t[0]
        neighbors = ast.literal_eval(t[1]) 
        self.increment_counter('graph', 'nodes', amount=1)
        self.increment_counter('graph', 'links', amount=len(neighbors))
        yield len(neighbors), node
            
    def combiner(self, key, values):
        l = [v for v in values]
        yield key,len(l)

    def reducer_init(self):
        self.max_link_bucket = 0
        self.max_link_num_nodes = 0
        self.min_link_bucket = 0
        self.min_link_num_nodes = sys.maxint
        
    def reducer(self, key, values):
        n_nodes = sum(values)
        
        if n_nodes > self.max_link_num_nodes:
            self.max_link_bucket = key
            self.max_link_num_nodes = n_nodes
            
        if n_nodes < self.min_link_num_nodes:
            self.min_link_bucket = key
            self.min_link_num_nodes = n_nodes
        
        self.increment_counter('graph', 'link_buckets', amount=1)
        yield key,n_nodes
        
    def reducer_final(self):
        c_min = '[MIN_NUM_NODES]{0}'.format(self.min_link_bucket)
        self.increment_counter('graph', c_min, amount=self.min_link_num_nodes)
        c_max = '[MAX_NUM_NODES]{0}'.format(self.max_link_bucket)
        self.increment_counter('graph', c_max, amount=self.max_link_num_nodes)
            
    
if __name__ == '__main__':
    Graph_EDA.run()