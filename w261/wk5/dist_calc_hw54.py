from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import sys
import ast
from sets import Set
import urllib2
import math


class DistanceCalc(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                  jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1n',
                            }
                   )
        ]
    
    def mapper_init(self):
        # Load the file into memory
        self.stripes = {}
        f = urllib2.urlopen("https://s3-us-west-2.amazonaws.com/ucb-mids-mls-sayantan-satpati/hw54/word_cooccur/frequent_stripes.txt")
        for line in f.readlines():
            tokens = line.strip().split('\t')
            self.stripes[tokens[0].replace("\"","")] = ast.literal_eval(tokens[1])
      
        sys.stderr.write('### of stripes: {0}'.format(len(self.stripes)))

    def mapper(self, _, line):        
        tokens = line.strip().split('\t')
        key = tokens[0].replace("\"","")
        dict_pairs = ast.literal_eval(tokens[1])
        s1 = Set(dict_pairs.keys())
        for n_key, n_dict_pairs in self.stripes.iteritems():
            # Do distance calc for only (a,b) but not (b,a) --> Redundant
            if key > n_key:
                continue
                
            s2 = Set(n_dict_pairs.keys())
            
            # Calculate Euclidean Distance
            # Get the union of keys from both stripes
            union_keys = s1.union(s2)
            
            squared_distance = 0
            for k in union_keys:
                squared_distance += (dict_pairs.get(k, 0) - n_dict_pairs.get(k, 0)) ** 2
                
            yield math.sqrt(squared_distance), (key, n_key, 'E')
            
            # Calculate Cosine Distance
            # Get the intersection of keys from both stripes
            intersection_keys = s1.intersection(s2)
            
            dot_x_y = 0
            for k in intersection_keys:
                dot_x_y += dict_pairs[k] - n_dict_pairs[k]
                
            norm_x = 0
            for k in s1:
                norm_x += dict_pairs[k] * dict_pairs[k]
                
            norm_y = 0
            for k in s2:
                norm_y += n_dict_pairs[k] * n_dict_pairs[k]
                
            cosine_dist = float(dot_x_y) / (math.sqrt(norm_x) * math.sqrt(norm_y))
                            
            yield cosine_dist, (key, n_key, 'C')

if __name__ == '__main__':
    DistanceCalc.run()