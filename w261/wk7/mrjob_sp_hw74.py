from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import sys
import ast

'''
Record Emitted by Mapper/Reducer:
Node <TAB> NULL|Neighbor Dict,Distance,Parent/Child,V|Q|U
'''

class ShortestPath_AWS(MRJob):
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer)
        ]
    
    def val1(self,ngbr,dist,parent,child,state):
        return '{0}|{1}|{2}/{3}|{4}'.format(ngbr,str(dist),parent,child,state)
    
    def val2(self,ngbr,dist,path,state):
        return '{0}|{1}|{2}|{3}'.format(ngbr,str(dist),path,state)
    
    def mapper_init(self):
        self.start_node = get_jobconf_value('start_node')
        self.stop_node = get_jobconf_value('stop_node')
        sys.stderr.write('### Start/Frontier Node: {0}\n'.format(self.start_node))
        sys.stderr.write('### Stop: {0}\n'.format(self.stop_node))

    def mapper(self, _, line):
        line = line.replace("\"","")
        # Passed only to first iteration
        t = line.strip().split('\t')
        node = t[0]
        #sys.stderr.write('[M] {0}\n'.format(line))
        if self.start_node: # First frontier
            neighbors = ast.literal_eval(t[1])    
            if node == self.start_node:
                # Mark Node as Visited
                yield node, self.val1(neighbors,0,node,node,'V')
                self.increment_counter('graph', 'nodes', amount=1)
                
                # Open/Emit Frontiers
                for k,v in neighbors.iteritems():
                    self.increment_counter('graph', 'frontiers', amount=1)
                    self.increment_counter('graph', 'links', amount=1)
                    yield k, self.val1('NULL',v,node,k,'Q')
            else:
                # Rest Passthrough
                yield node, self.val1(neighbors,sys.maxint,'NULL','NULL','U')
        else: # Not First
            t1 = t[1].split("|")
            neighbors = {}
            if t1[0] != 'NULL':
                neighbors = ast.literal_eval(t1[0])
            dist = t1[1]
            path = t1[2]
            status = t1[3]
            if status == 'Q':
                # Mark Node as Visited
                yield node, self.val2(neighbors,dist,path,'V')
                self.increment_counter('graph', 'nodes', amount=1)
                
                # Signal Driver to Stop Processing anymore
                if node == self.stop_node:
                    self.increment_counter('graph', 'action_stop', amount=1)
                
                # Open/Emit Frontiers if NOT Visited
                for k,v in neighbors.iteritems():
                    self.increment_counter('graph', 'frontiers', amount=1)
                    self.increment_counter('graph', 'links', amount=1)
                    yield k, self.val1('NULL', int(dist) + int(v), path, k,'Q')
            else:
                yield t[0], t[1] # Passthrough (Rest)
            
    def combiner(self, key, counts):
        pass

    def reducer(self, key, values):
        '''
        Passed to Reducer would be either of these combinations:
        1. Visited and Frontiers (Merge based on distance)
        2. Frontiers and Unvisited (Merge)
        3. Unvisited
        '''
        vList = [value for value in values]
        # Set min distance to integer max
        min_dist = sys.maxint
        
        if len(vList) == 1:
            yield key, vList[0]
        else:
            neighbors = None
            dist = None
            path = None
            status = None
            
            for value in vList:
                #sys.stderr.write('[R1] {0} : {1}\n'.format(key,value))
                t = value.split("|")
                #sys.stderr.write('[R2] D:{0},MD:{1}\n'.format(dist,min_dist))

                if int(t[1]) < min_dist:
                    dist = t[1]
                    path = t[2]
                    status = t[3]
                    min_dist = int(t[1])
                    
                if t[3] == 'V' or t[3] == 'U':
                    neighbors = ast.literal_eval(t[0])
                    
                #sys.stderr.write('[R3] {0}\n'.format(self.val2(neighbors,dist,path,status)))
                
            #sys.stderr.write('[R4] {0} # {1} \n\n'.format(key, self.val2(neighbors,dist,path,status)))
            yield key, self.val2(neighbors,dist,path,status)    
            
    
if __name__ == '__main__':
    ShortestPath_AWS.run()