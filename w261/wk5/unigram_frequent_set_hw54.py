from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class UnigramFrequentSet(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        tokens = line.strip().split('\t')
        unigrams = tokens[0].split()
        for unigram in unigrams:
            yield unigram,1

    def combiner(self, unigram, counts):
        yield unigram, sum(counts)
        
  
    def reducer(self, unigram, counts):
        total_count = sum(counts)
        if total_count >= 10000:
            yield unigram, total_count

if __name__ == '__main__':
    UnigramFrequentSet.run()