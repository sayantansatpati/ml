from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.compat import get_jobconf_value
import sys
import ast
from numpy import log1p, exp, log
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto

class PageRank_AWS(MRJob):
    
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
        aws_access_key_id = get_jobconf_value('aws_access_key_id')
        aws_secret_access_key = get_jobconf_value('aws_secret_access_key')
        
        adj_list = None
        total_score = 0

        for value_type, value in values:
            if value_type == 'GRAPH':
                adj_list = value
            else:
                assert value_type == 'SCORE'
                total_score += value
                #total_score = sum_log(total_score, value)
                
        # Write Special Key to S3
        if key == 'DANGLING':
            # Write accumulated Dangling Score in a S3 Key
            try:
                conn = S3Connection(aws_access_key_id,aws_secret_access_key)
                bucket = conn.get_bucket('w261')
                k = Key(bucket)
                k.key = 'hw93/dangling_mass/{0}'.format(i) # Same as iteration
                k.set_contents_from_string(str(total_score))
            except boto.exception.S3ResponseError as err:
                sys.stderr.write(err)
                sys.exit(1)
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
        aws_access_key_id = get_jobconf_value('aws_access_key_id')
        aws_secret_access_key = get_jobconf_value('aws_secret_access_key')
        
        self.dangling_mass = 0
        
        # Read Dangling Mass from S3 Bucket
        try:
            conn = S3Connection(aws_access_key_id,aws_secret_access_key)
            bucket = conn.get_bucket('w261')
            k = Key(bucket)
            k.key = 'hw93/dangling_mass/{0}'.format(i) # Same as iteration
            self.dangling_mass = float(k.get_contents_as_string())
        except boto.exception.S3ResponseError as err:
            sys.stderr.write(err)
            sys.exit(1)
        
        sys.stderr.write('[{0}][M_D] DANGLING MASS: {1}\n'.format(i, self.dangling_mass))
        
    def mapper_dangling(self, key, value):
        #sys.stderr.write('[M_D] {0}, {1} \n'.format(key, value))
        i = int(get_jobconf_value('iteration'))
        key = key.replace("\"","")
        key = key.replace("\\","")
        adj_list = ast.literal_eval(str(value))
        
        if self.dangling_mass > 0:
            nodes = int(get_jobconf_value('nodes'))
            teleportation = float(get_jobconf_value('teleportation'))
            score = adj_list['score']
            modified_score = (teleportation / nodes) + (1 - teleportation) * ((self.dangling_mass / nodes) + score)
            #modified_score = sum_log((teleportation / nodes), (1 - teleportation)*(self.dangling_mass / nodes))
            #modified_score = sum_log(modified_score, (1 - teleportation)*score)
            adj_list['score'] = modified_score
            
        yield key, adj_list
        

   
if __name__ == '__main__':
    PageRank_AWS.run()