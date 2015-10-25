from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import sys
import ast

'''
Record Emitted by Mapper/Reducer:
Node <TAB> NULL|Neighbor Dict,Distance,Parent/Child,V|Q|U
'''

class ShortestPath(MRJob):
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
        self.frontier_node = get_jobconf_value('frontier_node')
        #sys.stderr.write('### Frontier Node: {0}\n'.format(self.frontier_node))

    def mapper(self, _, line):
        line = line.replace("\"","")
        # Passed only to first iteration
        t = line.strip().split('\t')
        node = t[0]
        #sys.stderr.write('[M] {0}\n'.format(line))
        if self.frontier_node:
            neighbors = ast.literal_eval(t[1])    
            if node == self.frontier_node:
                # Mark Node as Visited
                yield node, self.val1(neighbors,0,node,node,'V')
                
                # Open/Emit Frontiers
                for k,v in neighbors.iteritems():
                    self.increment_counter('graph', 'frontiers', amount=1)
                    yield k, self.val1('NULL',v,node,k,'Q')
            else:
                # Rest Passthrough
                yield node, self.val1(neighbors,sys.maxint,'NULL','NULL','U')
        else:
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
                
                # Open/Emit Frontiers
                for k,v in neighbors.iteritems():
                    self.increment_counter('graph', 'frontiers', amount=1)
                    yield k, self.val1('NULL', int(dist) + int(v), path, k,'Q')
            else:
                yield t[0], t[1] # Passthrough (Rest)
            
    def combiner(self, key, counts):
        pass

    def reducer(self, key, values):
        vList = [value for value in values]
        
        if len(vList) == 1:
            yield key, vList[0]
        else:
            min_dist = sys.maxint
            neighbors = None
            dist = None
            path = None
            status = None
            
            for value in vList:
                #sys.stderr.write('[R] {0} : {1}\n'.format(key,value))
                t = value.split("|")

                # Eliminate processing nodes which are already visited
                if t[3] == 'V':
                    neighbors = ast.literal_eval(t[0])
                    dist = t[1]
                    path = t[2]
                    status = t[3]
                
                if t[3] == 'Q':
                    # Take the shortest path
                    if dist < min_dist:
                        dist = t[1]
                        path = t[2]
                        status = t[3]
                        min_dist = dist
                        
                if t[3] == 'U':
                    neighbors = ast.literal_eval(t[0])
                    
        
            yield key, self.val2(neighbors,dist,path,status)    
            
    
if __name__ == '__main__':
    ShortestPath.run()