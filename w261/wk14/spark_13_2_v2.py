import ast
import pprint
import sys
from math import log, exp, log1p
from pyspark import SparkContext
from pyspark import SparkConf

#global Var
g_dangling_mass = None

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
    #global g_dangling_mass
    l = [(t[0], 0)]
    w = t[1][1]
    adj_list = t[1][0]
    key = None
    if len(adj_list) == 0:
        #g_dangling_mass += w
        l.append(('DANGLING', w))
    else:
        for n in adj_list:
            l.append((n, w/len(adj_list)))
    return l

def page_rank(w, n, dangling_mass, LH, RH):
    # w = (tp / n) + (1 - tp) * ((dangling_mass/n) + w)
    # OR
    # w = (tp / n) + ((1 - tp)/n) * (dangling_mass + wn)
    # OR
    # w = LH + RH * (dangling_mass + wn)
    # OR
    # w = LH + exp(log(RH) + log(dangling_mass + wn))
    
    w = LH + exp(log(RH) + log(dangling_mass + w * n))
    return w

if __name__ == '__main__':
    print 'Number of arguments:', len(sys.argv), 'arguments.'
    print 'Argument List:', str(sys.argv)
    
    if len(sys.argv) != 5:
        print 'Incorrect number of arguments passed, Aborting...'
        sys.exit(1)
        
    # Init Spark Context
    #conf = SparkConf()
    sc = SparkContext(appName="Page Rank")
    
    lines = sc.textFile(sys.argv[1]).map(parse_line)
    #print '\n### Original Dataset:'
    #pprint.pprint(lines.sortByKey().collect())

    # Cache the linkys (They are Bigger)
    num_partitions = int(sys.argv[4])
    links = lines.flatMap(preproc) \
                 .reduceByKey(lambda x, y: x + y) \
                 .partitionBy(num_partitions) \
                 .cache()
    #print '\n### Pre-Processed Dataset (Links):'
    #pprint.pprint(links.collect())

    # Use map values to partition ranks in the same way as links
    n = links.count()
    ranks = links.mapValues(lambda x: float(1)/n)
    #print '\n### Inital Ranks:'
    #pprint.pprint(ranks.collect())

    #sum_partial_diff_PR = float('inf')
    #Teleportation & Damping Factor (Calculate & Pass)
    tp=0.15
    LH = tp / n
    RH = (1 - tp) / n
    sys.stderr.write('### Teleportation: {0}, LH: {1}, RH: {2}'.format(tp, LH, RH))
    cnt = 1

    #while sum_partial_diff_PR > .005:
    while cnt <= int(sys.argv[3]):
        #global g_dangling_mass
        #g_dangling_mass = sc.accumulator(0.0)
        contribs = links.join(ranks) \
                    .flatMap(contributions) \
                    .reduceByKey(lambda x, y: x + y)
        dangling_mass = contribs.lookup('DANGLING')[0]
        ranks = contribs.filter(lambda x: x[0] != 'DANGLING') \
                        .mapValues(lambda x: page_rank(x, n, dangling_mass, LH, RH))
        sys.stderr.write('\n[{0}] Dangling Mass: {1}'.format(cnt, dangling_mass))
    
        cnt += 1

    sc.parallelize(ranks.takeOrdered(100, key=lambda x: -x[1])).saveAsTextFile(sys.argv[2])
    
    sc.stop()