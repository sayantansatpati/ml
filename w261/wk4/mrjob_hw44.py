from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFrequentVisitor(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
             MRStep(reducer_init=self.reducer_init,
                    reducer=self.reducer_frequent_visitor)
        ]

    def mapper(self, _, line):
        tokens = line.strip().split(",")
        yield (tokens[1],tokens[4]), 1

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        yield key, sum(counts)
        
    # 2nd Pass
        
    def reducer_init(self):
        # Reads the 'url' file into a Dict for displaying additional information
        self.last_key = None
        self.last_max_count = 0
        self.pageDict = {}
        with open('url','r') as f:
            for line in f:
                tokens = line.strip().split(",")
                self.pageDict[tokens[1]] = tokens[4]
                
    def reducer_frequent_visitor(self, key, counts):
        if not self.last_key:
            self.last_key = key[0]
            yield key, sum(counts)
        else:
            if self.last_key != key[0]:
                yield key, sum(counts)
                self.last_key = key[0]

if __name__ == '__main__':
    MRFrequentVisitor.run()