from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import sys
import ast
from sets import Set
import math


class DistanceCalc(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper)
        ]
    
    def mapper_init(self):
        # Load the file into memory
        self.stripes = {}
        with open('frequent_stripes.txt', 'r') as f:
            for line in f:
                tokens = line.strip().split('\t')
                self.stripes[tokens[0].replace("\"","")] = ast.literal_eval(tokens[1])
        sys.stderr.write('### of stripes: {0}'.format(len(self.stripes)))

    def mapper(self, _, line):        
        tokens = line.strip().split('\t')
        key = tokens[0].replace("\"","")
        dict_pairs = ast.literal_eval(tokens[1])
        for n_key, n_dict_pairs in self.stripes.iteritems():
            # Do distance calc for only (a,b) but not (b,a) --> Redundant
            if key > n_key:
                continue
            
            s1 = Set(dict_pairs.keys())
            s2 = Set(n_dict_pairs.keys())
            common_keys = s1.intersection(s2)
            l_uniq = s1.difference(s2)
            r_uniq = s2.difference(s1)
            
            euclidean_distance = 0
            for k in common_keys:
                euclidean_distance += (dict_pairs.get(k) - n_dict_pairs.get(k)) ** 2
            for k in l_uniq:
                euclidean_distance += dict_pairs.get(k) ** 2
            for k in r_uniq:
                euclidean_distance += n_dict_pairs.get(k) ** 2
                
            yield (key, n_key, 'E'), math.sqrt(euclidean_distance)

if __name__ == '__main__':
    DistanceCalc.run()
