import ast
import pprint
import sys
from math import log, exp
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.evaluation import BinaryClassificationMetrics


def parse_line(line):
    t = ast.literal_eval(line)
    return LabeledPoint(t[0], SparseVector(t[1][0], t[1][1], t[1][2]))

def computeLogLoss(p, y):
    """Calculates the value of log loss for a given probabilty and label.

    Note:
        log(0) is undefined, so when p is 0 we need to add a small value (epsilon) to it
        and when p is 1 we need to subtract a small value (epsilon) from it.

    Args:
        p (float): A probabilty between 0 and 1.
        y (int): A label.  Takes on the values 0 and 1.

    Returns:
        float: The log loss value.
    """
    epsilon = 10e-12
    if p == 0:
        p = p + epsilon
    if p == 1:
        p = p - epsilon
    return -(y * log(p) + (1-y) * log(1-p))

def getP(x, w, intercept):
    """Calculate the probability for an observation given a set of weights and intercept.

    Note:
        We'll bound our raw prediction between 20 and -20 for numerical purposes.

    Args:
        x (SparseVector): A vector with values of 1.0 for features that exist in this
            observation and 0.0 otherwise.
        w (DenseVector): A vector of weights (betas) for the model.
        intercept (float): The model's intercept.

    Returns:
        float: A probability between 0 and 1.
    """
    rawPrediction = x.dot(w) + intercept

    # Bound the raw prediction value
    rawPrediction = min(rawPrediction, 20)
    rawPrediction = max(rawPrediction, -20)
    return 1 / (1 + exp(-rawPrediction))

def evaluateResults(model, data):
    """Calculates the log loss for the data given the model.

    Args:
        model (LogisticRegressionModel): A trained logistic regression model.
        data (RDD of LabeledPoint): Labels and features for each observation.

    Returns:
        float: Log loss for the data.
    """
    labelPredictions = data.map(lambda lp: (getP(lp.features, model.weights, model.intercept), lp.label))
    return labelPredictions.map(lambda (p,l): computeLogLoss(p,l)).sum() / labelPredictions.count()

def metrics(model, data, label):
    labelsAndScores = data.map(lambda lp:
                            (lp.label, getP(lp.features, model0.weights, model0.intercept)))
    
    metrics = BinaryClassificationMetrics(labelsAndScores)
    log_loss = evaluateResults(model0, OHETrainData)
    auc = metrics.areaUnderROC
    sys.stderr.write('\n [{0}] LogLoss: {1}'.format(label, log_loss))
    sys.stderr.write('\n [{0}] AUC: {1}\n'.format(label, auc))
    return (label, log_loss, auc)

if __name__ == '__main__':
    sys.stderr.write('\nNumber of arguments: {0}'.format(len(sys.argv)))
    sys.stderr.write('\nArgument List: {0}'.format(sys.argv))
    
    if len(sys.argv) != 5:
        print 'Incorrect number of arguments passed, Aborting...'
        sys.exit(1)
        
    # Init Spark Context
    #conf = SparkConf()
    sc = SparkContext(appName="Logistic Regression")
    
    OHETrainData = sc.textFile(sys.argv[1]).map(parse_line).cache()
    OHETestData = sc.textFile(sys.argv[2]).map(parse_line).cache()
    OHEValidateData = sc.textFile(sys.argv[3]).map(parse_line).cache()
    
    #print '\n', OHETrainData.take(3)
    
    # Tune hyperparameters
    numIters = 50
    stepSize = [1,10,25]
    regParam = [1e-3,1e-6,1e-9]
    regType = 'l2'
    includeIntercept = True
    
    l = []
    for rp in regParam:
        for ss in stepSize:
            model0 = LogisticRegressionWithSGD.train(OHETrainData, iterations=numIters, step=ss, 
                                           regParam=rp, regType=regType, intercept=includeIntercept)
            sortedWeights = sorted(model0.weights)



            sys.stderr.write('\n### Model Intercept: {0}'.format(model0.intercept))
            sys.stderr.write('\n### Model Weights (First 5): {0}\n'.format(sortedWeights[:5]))

            l.append(metrics(model0, OHETrainData, '[{0}][{1}]TRAIN'.format(rp,ss)))
            l.append(metrics(model0, OHETestData, '[{0}][{1}]TEST'.format(rp,ss)))
            l.append(metrics(model0, OHEValidateData, '[{0}][{1}]VALIDATE'.format(rp,ss)))
    
    sc.parallelize(l).saveAsTextFile(sys.argv[4])