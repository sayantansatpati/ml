from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.compat import get_jobconf_value
import sys
import ast


class PageRankDanglingMassPostProc(MRJob):
    
    INPUT_PROTOCOL = RawProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper)
        ]
    

    def mapper(self, key, value):
        nodes = int(get_jobconf_value('nodes'))
        dangling_mass = float(get_jobconf_value('dangling_mass'))
        teleportation = float(get_jobconf_value('teleportation'))
        #sys.stderr.write('[M] {0}, {1} \n'.format(key, value))
        key = key.replace("\"","")
        key = key.replace("\\","")
        neighbors = ast.literal_eval(value)
        
        score = float(neighbors['score'])
        
        modified_score = teleportation / nodes + (1 - teleportation) * ( (dangling_mass / nodes) + score)
        
        print '{0}, {1}, {2}'.format(score, modified_score, dangling_mass)
        
        neighbors['score'] = modified_score
        
        yield key, neighbors
               
   
if __name__ == '__main__':
    PageRankDanglingMassPostProc.run()