from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.compat import get_jobconf_value
import sys
import ast
from numpy import log1p, exp, log

class TopicPageRank(MRJob):
    
    INPUT_PROTOCOL = RawProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer),
            MRStep(mapper_init=self.mapper_dangling_init,
                mapper=self.mapper_dangling)
        ]
    

    def mapper(self, key, value):
        nodes = int(get_jobconf_value('nodes'))
        i = int(get_jobconf_value('iteration'))
        #sys.stderr.write('[M] {0}, {1} \n'.format(key, value))
        key = key.replace("\"","")
        key = key.replace("\\","")
        adj_list = ast.literal_eval(value)
      
        score = 0
        l = 0
        
        if 'score' in adj_list.keys():
            # Previous Mass/Page Rank
            score = adj_list['score']
            l = len(adj_list) - 1
        else: # First iteration ('score' not yet part of the adjacency list!)
            # Start with uniform probability distribution
            score = 1.0 / nodes
            l = len(adj_list)
            adj_list['score'] = score
            
        if l == 0: # Only 'score' & no out links [Dangling!]
            sys.stderr.write('[{0}][M] "DANGLING MASS" | {1} | {2}\n'.format(i, key, score))
            # Emit using a special key; Accumlate in Reducer;Distribute in the next MRJob
            yield 'DANGLING', ('SCORE', score)
       
        # Emit the Graph Structure
        yield key, ('GRAPH', adj_list)
                    
        # Emit the new Mass/Page Rank
        for n in adj_list:
            if n != 'score':
                yield n, ('SCORE', score/l)
                               
    def combiner(self, key, values):
        pass

        
    def reducer(self, key, values):     
        i = int(get_jobconf_value('iteration'))
        teleportation = float(get_jobconf_value('teleportation'))
        nodes = int(get_jobconf_value('nodes'))
        
        adj_list = None
        total_score = 0

        for value_type, value in values:
            if value_type == 'GRAPH':
                adj_list = value
            else:
                assert value_type == 'SCORE'
                total_score += value
                #total_score = sum_log(total_score, value)
                
        # Special Key
        if key == 'DANGLING':
            # Write accumulated Dangling Score in a file
            with open('/Users/ssatpati/0-DATASCIENCE/DEV/github/ml/w261/wk9/dangling.txt', 'w') as f:
                f.write('DANGLING\t{0}\n'.format(total_score))
        else:
            #total_score = (teleportation / nodes) + ((1 - teleportation) * total_score)
            #total_score = sum_log((teleportation / nodes), ((1 - teleportation) * total_score))
            if adj_list:
                adj_list['score'] = total_score
            else:
                adj_list = {'score': total_score}
    
            #sys.stderr.write('[R2] {0} | {1} | {2}\n\n'.format(key, total_score, adj_list))
            yield key, adj_list
       

    def mapper_dangling_init(self):
        i = int(get_jobconf_value('iteration'))
        
        # Page/Topic Mapping & Topic Counts for each Topic.
        self.topics = {}
        self.topicCounts = {}
        with open('randNet_topics.txt') as f:
            for l in f:
                t = l.split('\t')
                self.topics[t[0].strip()] = t[1].strip()
                
        for k,v in self.topics.iteritems():
            self.topicCounts[v] = self.topicCounts.get(v, 0) + 1
        
        self.dangling_mass = 0
        f_dangling = '/Users/ssatpati/0-DATASCIENCE/DEV/github/ml/w261/wk9/dangling.txt'
        try:
            with open(f_dangling, 'r') as f:
                l = f.readlines()
                if l:
                    self.dangling_mass = float(l[0].split('\t')[1])
            open(f_dangling, 'w').close()
        except Exception as e:
            pass
        #sys.stderr.write('[{0}][M_D] DANGLING MASS: {1}\n'.format(i, self.dangling_mass))
        
    def mapper_dangling(self, key, value):
        # Topic of Current Node
        topic = get_jobconf_value('topic')
        # Number of Nodes in same Topic as current Node
        n_nodes_topic = self.topicCounts.get(topic, 0)
        
        #sys.stderr.write('[M_D] {0}, {1}, {2} \n'.format(key, topic, n_nodes_topic)) 
        
        i = int(get_jobconf_value('iteration'))
        key = key.replace("\"","")
        key = key.replace("\\","")
        adj_list = ast.literal_eval(str(value))
        
        nodes = int(get_jobconf_value('nodes'))
        teleportation = float(get_jobconf_value('teleportation'))
        topic_bias = float(get_jobconf_value('topic_bias'))
        
        score = adj_list['score']
        
        '''
            Adjust for Topic Bias
            Random Surfer selects Nodes in same Topic as current node using a Topic Bias (> 0.5: Topic Sensitive)
        '''
        if topic != '0':
            random_topic_jump = teleportation * ((topic_bias/n_nodes_topic) + ((1 - topic_bias)/ (nodes - n_nodes_topic)))
            modified_score = random_topic_jump + (1 - teleportation) * ((self.dangling_mass / nodes) + score)
        else:
            modified_score = (teleportation / nodes) + (1 - teleportation) * ((self.dangling_mass / nodes) + score)
        
        #modified_score = (teleportation / nodes) + (1 - teleportation) * ((self.dangling_mass / nodes) + score)
        #modified_score = sum_log((teleportation / nodes), (1 - teleportation)*(self.dangling_mass / nodes))
        #modified_score = sum_log(modified_score, (1 - teleportation)*score)
        adj_list['score'] = modified_score
            
        yield key, adj_list
        

   
if __name__ == '__main__':
    TopicPageRank.run()