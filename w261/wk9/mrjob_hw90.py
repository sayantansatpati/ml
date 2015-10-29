from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.compat import get_jobconf_value
import sys
import ast

'''
Sample Input Data:
1	0.2,[2,4]
2	0.2,[3,5]
3	0.2,[4]
4	0.2,[5]
5	0.2,[1,2,3]
'''

class PageRank(MRJob):
    
    INPUT_PROTOCOL = RawProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer)
        ]
    

    def mapper(self, key, value):
        value = value.strip().replace("\"","")
        t = value.split("|")
        
        #sys.stderr.write('[M] {0} | {1} | {2}\n'.format(key, t[0], t[1]))
        
        node = key
        score = t[0]
        neighbors = ast.literal_eval(t[1])
        
        # Emit the Graph Structure
        yield int(node), ('NODE', value)
        
        # Emit the mass
        for n in neighbors:
            yield n, ('SCORE', float(score)/len(neighbors))
                   
        #self.increment_counter('page_rank', 'dangling_node', amount=1)
            
    def combiner(self, key, values):
        pass

        
    def reducer(self, key, values):
        prev_score = None
        adj_list = None
        total_score = 0

        for value_type, value in values:
            #sys.stderr.write('[R1] {0} | {1} | {2}\n'.format(key, value_type, value))
            if value_type == 'NODE':
                t = value.strip().split("|")
                prev_score = t[0]
                adj_list = t[1]
            else:
                assert value_type == 'SCORE'
                total_score += value
        
        '''
        node['prev_score'] = node['score']

        d = self.options.damping_factor
        node['score'] = 1 - d + d * total_score
        '''

        #sys.stderr.write('[R2] {0} | {1} | {2}\n\n'.format(key, total_score, adj_list))
        yield key, '{0}|{1}'.format(total_score, adj_list)

   
if __name__ == '__main__':
    PageRank.run()