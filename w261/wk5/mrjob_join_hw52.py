from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFrequentVisitor(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
             MRStep(mapper=self.mapper_frequent_visitor,
                   reducer_init=self.reducer_frequent_visitor_init,
                   reducer=self.reducer_frequent_visitor)
        ]

    def mapper(self, _, line):
        tokens = line.strip().split(",")
        key = "{0},{1}".format(tokens[1],tokens[4])
        yield key, 1

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        yield key, sum(counts)
        
    # 2nd Pass
    
    def mapper_frequent_visitor(self, key, value):
        tokens = key.strip().split(",")
        modified_key = "{0},{1}".format(tokens[0],value)
        yield modified_key, tokens[1]
     
    
    def reducer_frequent_visitor_init(self):
        # Reads the 'url' file into a Dict for displaying additional information
        self.last_page = None
        self.pageDict = {}
        with open('url','r') as f:
            for line in f:
                tokens = line.strip().split(",")
                self.pageDict[tokens[1]] = tokens[4]
                
    def reducer_frequent_visitor(self, key, values):
        tokens = key.strip().split(",")
        page = tokens[0]
        visits = int(tokens[1])
        
        if self.last_page != page:
            self.last_page = page
            # values might be a list, if there is a tie for same key => (p1, 1000), [v1,v2,v3..]
            for value in values:
                k = '{0},{1}'.format(page, 
                                    self.pageDict.get(page, 'NA').replace("\"",""))
                v = '{0},{1}'.format(visits,
                                    value)
                yield k,v

if __name__ == '__main__':
    MRFrequentVisitor.run()