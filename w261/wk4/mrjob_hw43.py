from mrjob.job import MRJob
from mrjob.step import MRStep


class MRVistedPagesCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_top_5)
        ]

    def mapper(self, _, line):
        tokens = line.strip().split(",")
        yield tokens[1], 1

    def combiner(self, page_visted, counts):
        yield page_visted, sum(counts)

    def reducer(self, page_visted, counts):
        yield None, (sum(counts), page_visted)

    # discard the key; it is just None
    def reducer_find_top_5(self, _, page_visted_pairs):
        # Store all the keys into memory (Assumption: Can be loaded into memory)
        pairs = []
        for p in page_visted_pairs:
            pairs.append(p)
        pairs.sort(key=lambda x: x[0], reverse=True)
            
        for p in pairs[:5]:
            yield p[1],p[0]
            

if __name__ == '__main__':
    MRVistedPagesCount.run()