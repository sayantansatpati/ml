import ast
import pprint
import sys
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
    l = [(t[0], 0)]
    w = t[1][1]
    adj_list = t[1][0]
    key = None
    if len(adj_list) == 0:
        l.append(('DANGLING', w))
    else:
        for n in adj_list:
            l.append((n, w/len(adj_list)))
    return l

def page_rank(t, n, dangling_mass, tp=0.15):
    w = t[1]
    w = (tp / n) + (1 - tp) * ((dangling_mass/n) + w)
    return (t[0], w)

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

    links = lines.flatMap(preproc).reduceByKey(lambda x, y: x + y).cache()
    n = links.count()
    
    ranks = links.map(lambda x: (x[0], float(1)/n))

    sum_partial_diff_PR = float('inf')
    cnt = 1

    #while sum_partial_diff_PR > .005:
    while cnt <= int(sys.argv[3]):
        contribs = links.join(ranks).flatMap(contributions).reduceByKey(lambda x, y: x + y).cache()
        dangling_mass = contribs.lookup('DANGLING')
        ranks_updated = contribs.filter(lambda x: x[0] != 'DANGLING').map(lambda x: page_rank(x, n, dangling_mass[0]))
        sys.stderr.write('\n[Iteration: {0}] Dangling Mass: {1}'.format(cnt, dangling_mass[0]))
        
        ranks = ranks_updated
        cnt += 1

    sc.parallelize(ranks.map(lambda x: (x[0],round(x[1],3))).takeOrdered(3, key=lambda x: -x[1])).saveAsTextFile(sys.argv[2])
    
    sc.stop()