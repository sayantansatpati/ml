from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import sys
import ast
import urllib2
import math


class CSMR(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer
                   )
        ]
    
    def mapper_init(self):
        # Load the file into memory
        self.counter = 0
        self.stripes = {}
        '''
        f = urllib2.urlopen("https://s3-us-west-2.amazonaws.com/ucb-mids-mls-sayantan-satpati/hw54/word_cooccur/frequent_stripes.txt")
        for line in f.readlines():
            tokens = line.strip().split('\t')
            self.stripes[tokens[0].replace("\"","")] = ast.literal_eval(tokens[1])
            self.increment_counter('distance', 'num_stripes_loaded', amount=1)
        '''
        with open('frequent_stripes.txt','r') as f:
            for line in f:
                tokens = line.strip().split('\t')
                self.stripes[tokens[0].replace("\"","")] = ast.literal_eval(tokens[1])
                self.increment_counter('distance', 'num_stripes_loaded', amount=1)
      
        sys.stderr.write('### of stripes loaded into mapper: {0}\n'.format(len(self.stripes)))
        
    def mapper(self, _, line):
        try:
            tokens = line.strip().split('\t')
            key = tokens[0].replace("\"","")
            dict_pairs = ast.literal_eval(tokens[1])
            
            # Calc norm for LHS
            norm_x = 0
            for k in dict_pairs.keys():
                norm_x += dict_pairs[k] * dict_pairs[k]
            
            for n_key, n_dict_pairs in self.stripes.iteritems():
                # TODO distance calc for only (a,b) but not (b,a) --> Redundant
                if key > n_key:
                    continue
                    
                # Calc norm for RHS
                norm_y = 0
                for k in n_dict_pairs.keys():
                    norm_y += n_dict_pairs[k] * n_dict_pairs[k]
                    
                # Yield the key & value pairs
                yield (key,n_key,norm_x,norm_y), (dict_pairs, n_dict_pairs)
        except Exception, e:
            sys.stderr.write("[Mapper]{0}:{1}\n".format(line, e))
            pass
        
    def reducer(self, key, value):
        try:
            # Only one value would be there
            dicts = next(value)
            
            # Calculate Cosine Distance
            # Get the intersection of keys from both stripes
            dot_x_y = 0
            for k in dicts[0].keys():
                if k in dicts[1].keys():
                    dot_x_y += dicts[0][k] * dicts[1][k]
                    
            distance = float(dot_x_y) / (math.sqrt(int(key[2])) * math.sqrt(int(key[3])))
            
            self.increment_counter('distance', 'num_cosine_distances', amount=1)
            yield (distance), (key[0], key[1])
            
        except Exception, e:
            sys.stderr.write("[Reducer]{0}:{1}\n".format(key, e))
            pass


if __name__ == '__main__':
    CSMR.run()