from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import sys


class MRFrequentVisitor1(MRJob):
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper)
        ]

    def mapper_init(self):
        self.pageVisitsDict = {}
        with open('anonymous-msweb.data.pp','r') as f:
            for line in f:
                tokens = line.strip().split(",")
                page = tokens[1]
                visitor = tokens[4]
                if page not in self.pageVisitsDict.keys():
                    self.pageVisitsDict[page] = {}
                if visitor not in self.pageVisitsDict[page].keys():
                    self.pageVisitsDict[page][visitor] = 0
                self.pageVisitsDict[page][visitor] = self.pageVisitsDict[page][visitor] + 1
            
    
    def mapper(self, _, line):
        tokens = line.strip().split(",")
        page = tokens[1]
        page_url = tokens[4].replace("\"","")
        if page in self.pageVisitsDict.keys():
            for k,v in self.pageVisitsDict[page].iteritems():
                
                key = '{0},{1}'.format(page, 
                                    page_url)
                value = '{0},{1}'.format(v,
                                     k)
                yield (key,value)
        else:
            k = '{0},{1}'.format(page, 
                                page_url)
            v = '{0},{1}'.format(0,
                                 'NA')
            yield (k,v)

if __name__ == '__main__':
    MRFrequentVisitor1.run()