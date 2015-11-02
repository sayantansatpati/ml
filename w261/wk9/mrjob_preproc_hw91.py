from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.compat import get_jobconf_value
import sys
import ast


class FilePreProc(MRJob):
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer)
        ]
    

    def mapper(self, _, line):
        t = line.strip().split('\t')
        adj_list = ast.literal_eval(t[1])
        
        yield t[0], adj_list
        
        for n in adj_list:
            yield n, '*'
          
    def reducer(self, key, values):  
        source = False
        adj_list = {}
        for v in values:
            if type(v) == type({}):
                source = True
                adj_list = v
        yield key, adj_list
   
if __name__ == '__main__':
    FilePreProc.run()