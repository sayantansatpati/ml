from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class LongestNgram(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_ngrams_len,
                   reducer=self.reducer_ngrams_len),
            MRStep(reducer=self.reducer_find_max_ngram)
        ]

    def mapper_ngrams_len(self, _, line):
        tokens = line.strip().split('\t')
        yield (tokens[0], len(tokens[0]))

  
    def reducer_ngrams_len(self, word, counts):
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_ngram(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)


if __name__ == '__main__':
    LongestNgram.run()