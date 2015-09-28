from numpy import argmin, array, random
from mrjob.job import MRJob
from mrjob.step import MRJobStep
from itertools import chain

#Calculate find the nearest centroid for data point 
def MinDist(datapoint, centroid_points):
    datapoint = array(datapoint)
    centroid_points = array(centroid_points)
    diff = datapoint - centroid_points 
    diffsq = diff*diff
    # Get the nearest centroid for each instance
    minidx = argmin(list(diffsq.sum(axis = 1)))
    return minidx

#Check whether centroids converge
def stop_criterion(centroid_points_old, centroid_points_new,T):
    oldvalue = list(chain(*centroid_points_old))
    newvalue = list(chain(*centroid_points_new))
    Diff = [abs(x-y) for x, y in zip(oldvalue, newvalue)]
    Flag = True
    for i in Diff:
        if(i>T):
            Flag = False
            break
    return Flag

class MRKmeans(MRJob):
    centroid_points=[]
    #k=3    
    def steps(self):
        return [
            MRJobStep(mapper_init = self.mapper_init, mapper=self.mapper,combiner = self.combiner,reducer=self.reducer)
               ]
    #load centroids info from file
    def mapper_init(self):
        self.centroid_points = [map(float,s.split('\n')[0].split(',')) for s in open("centroids_1.txt").readlines()]
        k = len(self.centroid_points)
        open('centroids_1.txt', 'w').close()
    #load data and output the nearest centroid index and data point 
    def mapper(self, _, line):
        D = (map(float,line.split(',')))
        #yield int(MinDist(D,self.centroid_points)), (D[0],D[1],1)
        yield int(MinDist(D[3:],self.centroid_points)), (D[3:],1)
    #Combine sum of data points locally
    def combiner(self, idx, inputdata):
        '''
        sumx = sumy = num = 0
        for x,y,n in inputdata:
            num = num + n
            sumx = sumx + x
            sumy = sumy + y
        yield idx,(sumx,sumy,num)
        '''
        num = 0
        sum_n = [0 for i in xrange(1000)]
        for d, n in inputdata:
            num = num + n
            sum_n = [x + y for x,y in zip(d,sum_n)]
        yield idx,(sum_n,num) 
    #Aggregate sum for each cluster and then calculate the new centroids
    def reducer(self, idx, inputdata): 
        centroids = []
        num = [0]*self.k 
        for i in range(self.k):
            #centroids.append([0,0])
            centroids.append([0 for i in xrange(1000)])
        '''
        for x, y, n in inputdata:
            num[idx] = num[idx] + n
            centroids[idx][0] = centroids[idx][0] + x
            centroids[idx][1] = centroids[idx][1] + y
        centroids[idx][0] = centroids[idx][0]/num[idx]
        centroids[idx][1] = centroids[idx][1]/num[idx]
        with open('Centroids.txt', 'a') as f:
            f.writelines(str(centroids[idx][0]) + ',' + str(centroids[idx][1]) + '\n')
        yield idx,(centroids[idx][0],centroids[idx][1])
        '''
        for d, n in inputdata:
            num[idx] = num[idx] + n
            for i in xrange(1000):
                centroids[idx][i] = centroids[idx][i] + d[i]
        for i in xrange(1000):
            centroids[idx][i] = centroids[idx][i]/num[idx]
            
        with open('centroids_1.txt', 'a') as f:
            f.writelines(",".join(centroids[idx]) + '\n')
        yield idx,(centroids[idx])
      
if __name__ == '__main__':
    MRKmeans.run()