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
        if not self.frontier_node:
            # Save a list of visited nodes
            self.visited = [s.strip() for s in 
                              open('visited.txt').readlines()]
            open('visited.txt', 'w').close()
            #sys.stderr.write('### Visited: {0}\n'.format(self.visited))
        #sys.stderr.write('### Frontier Node: {0}\n'.format(self.frontier_node))

    def mapper(self, _, line):
        line = line.replace("\"","")
        # Passed only to first iteration
        t = line.strip().split('\t')
        node = t[0]
        #sys.stderr.write('[M] {0}\n'.format(line))
        if self.frontier_node: # First frontier
            neighbors = ast.literal_eval(t[1])    
            if node == self.frontier_node:
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
                self.visited.append(node)
                
                # Open/Emit Frontiers if NOT Visited
                for k,v in neighbors.iteritems():
                    if k not in self.visited:
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
    ShortestPath.run()