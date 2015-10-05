from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import sys


class WordCoOccurrenceFrequentSet(MRJob):

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]
    
    def mapper_init(self):
        # Load the file into memory
        self.unigrams = {}
        with open('frequent_unigrams.txt', 'r') as f:
            for line in f:
                tokens = line.strip().split('\t')
                self.unigrams[tokens[1].replace("\"","")] = int(tokens[0])
        sys.stderr.write('### of unigrams: {0}'.format(self.unigrams))

    def mapper(self, _, line):
        tokens = line.strip().split('\t')
        # List of 5-grams
        words = tokens[0].split()
        # Filter 5-grams to only those in list
        words = [w for w in words if w in self.unigrams.keys()]
        l = len(words)
        for i in xrange(l):
            d = {}
            for j in xrange(l):
                if i != j:
                    d[words[j]] = d.get(words[j], 0) + 1
            # Emit word, stripe
            yield words[i],d

    def combiner(self, word, stripes):
        d = {}
        # Aggregate stripes
        for s in stripes:
            for k, v in s.iteritems():
                d[k] = d.get(k, 0) + v
        yield word,d
        
    def reducer(self, word, stripes):
        d = {}
        # Aggregate stripes
        for s in stripes:
            for k, v in s.iteritems():
                d[k] = d.get(k, 0) + v
        '''
        # Filter based on support count (Not a requirement!)
        d_final = {}
        for k,v in d.iteritems():
            if v >= 10000:
                d_final[k] = v
        '''
        # Combine stripes
        yield word,d

if __name__ == '__main__':
    WordCoOccurrenceFrequentSet.run()