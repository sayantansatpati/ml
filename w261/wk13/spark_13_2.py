import ast
import pprint
import sys
from math import log, exp, log1p
from pyspark import SparkContext
from pyspark import SparkConf

def u(s):
    return s.decode('utf-8')

def parse_line(line):
    tokens = line.split('\t')
    key = tokens[0]
    adj_list = ast.literal_eval(tokens[1])
    return (u(key), [u(k) for k,v in adj_list.iteritems()])

def preproc(t):
    l = [t]
    for x in t[1]:
        l.append((u(x),[]))
    return l

def contributions(t):
    adj_list = t[1][0]
    w = t[1][1]
    
    # Emit the Graph/AdjList
    l = [(t[0], (adj_list, 0.0))]
    
    #Emit the Weights
    if len(adj_list) == 0:
        l.append(('DANGLING', ([], w)))
    else:
        for n in adj_list:
            l.append((n, ([], float(w)/len(adj_list))))
    return l

def page_rank(t, n, dangling_mass, LH, RH):
    adj_list = t[1][0]
    w = t[1][1]
    
    # w = (tp / n) + (1 - tp) * ((dangling_mass/n) + w)
    # OR
    # w = (tp / n) + ((1 - tp)/n) * (dangling_mass + wn)
    # OR
    # w = LH + RH * (dangling_mass + wn)
    # OR
    # w = LH + exp(log(RH) + log(dangling_mass + wn))
    
    w = LH + exp(log(RH) + log(dangling_mass + w * n))
    return (t[0], (adj_list, w))

if __name__ == '__main__':
    sys.stderr.write('\nNumber of arguments: {0}'.format(len(sys.argv)))
    sys.stderr.write('\nArgument List: {0}'.format(sys.argv))
    
    if len(sys.argv) != 4:
        print 'Incorrect number of arguments passed, Aborting...'
        sys.exit(1)
        
    # Init Spark Context
    #conf = SparkConf()
    sc = SparkContext(appName="Page Rank")
    
    lines = sc.textFile(sys.argv[1]).map(parse_line)

    pr = lines.flatMap(preproc).reduceByKey(lambda x, y: x + y)
    n = pr.count()
    pr = pr.map(lambda x: (x[0],(x[1], float(1)/n)))
    
    #pprint.pprint(pr.collect())
    
    #Teleportation & Damping Factor (Calculate & Pass)
    tp=0.15
    LH = tp / n
    RH = (1 - tp) / n
    sys.stderr.write('### Teleportation: {0}, LH: {1}, RH: {2}'.format(tp, LH, RH))
    cnt = 1
    
    
    #while sum_partial_diff_PR > .005:
    while cnt <= int(sys.argv[3]):
        contribs = pr.flatMap(contributions).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))        
        #print '\n'
        #pprint.pprint(contribs.collect())
        dangling_mass = contribs.filter(lambda x: x[0] == 'DANGLING').collectAsMap()['DANGLING'][1]
        sys.stderr.write('\n[{0}] Dangling Mass: {1}'.format(cnt, dangling_mass))
        pr = contribs.filter(lambda x: x[0] != 'DANGLING').map(lambda x: page_rank(x, n, dangling_mass, LH, RH))   
        #print '\n'
        #pprint.pprint(contribs.collect())
        
        cnt += 1
    
    print '\n'
    
    #pprint.pprint(pr.sortByKey().collect())
    sc.parallelize(pr.map(lambda x: (x[0],(x[1][0], x[1][1])))
                        .takeOrdered(100, key=lambda x: -x[1][1])).saveAsTextFile(sys.argv[2])
    
    sc.stop()