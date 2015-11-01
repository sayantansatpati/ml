from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.compat import get_jobconf_value
import sys
import ast


class PageRank1(MRJob):
    
    INPUT_PROTOCOL = RawProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer)
        ]
    

    def mapper(self, key, value):
        nodes = int(get_jobconf_value('nodes'))
        #sys.stderr.write('[M] {0}, {1} \n'.format(key, value))
        key = key.replace("\"","")
        key = key.replace("\\","")
        neighbors = ast.literal_eval(value)
        
        score = 0
        l = 0
        if 'score' in neighbors.keys():
            score = neighbors['score']
            l = len(neighbors) - 1

            if l == 0: # Only 'score' & no neighbors (Start Accumulating Mass)
                sys.stderr.write('[M] "DANGLING" | {0} | {1}\n'.format(key, score))
                self.increment_counter('page_rank', 'dangling_mass', amount= int(score * 1000))
        else: # First iteration ('score' not yet part of the dicionary!)
            # Start with uniform distribution
            score = 1.0 / nodes
            neighbors['score'] = score
            l = len(neighbors)
            
        # Emit the Graph Structure
        yield key, ('NODE', neighbors)
        
        # Emit the mass
        for n in neighbors:
            if n != 'score':
                if n == 'A':
                    sys.stderr.write('[M1] A | {0} | {1} | {2}\n'.format(n, score, neighbors))
                yield n, ('SCORE', float(score)/l)
                   
        #self.increment_counter('page_rank', 'dangling_node', amount=1)
            
    def combiner(self, key, values):
        pass

        
    def reducer(self, key, values):        
        teleportation = float(get_jobconf_value('teleportation'))
        nodes = int(get_jobconf_value('nodes'))
        
        adj_list = None
        total_score = 0

        for value_type, value in values:
            if key == 'A':
                sys.stderr.write('[R1] {0} | {1} | {2}\n'.format(key, value_type, value))
            if value_type == 'NODE':
                adj_list = value
            else:
                assert value_type == 'SCORE'
                total_score += value
                
        # Update the score
        if adj_list:
            # Account for teleportation
            total_score = teleportation / nodes + (1 - teleportation) * total_score
            adj_list['score'] = total_score
            yield key, adj_list
        else:
            # Accumulate the mass from dangling nodes (First Time Only)
            sys.stderr.write('[R1] "DANGLING" | {0} | {1}\n'.format(key, value))
            adj_list = {'score': total_score}
            yield key, adj_list
        
        #sys.stderr.write('[R2] {0} | {1} | {2}\n\n'.format(key, total_score, adj_list))
        

   
if __name__ == '__main__':
    PageRank1.run()