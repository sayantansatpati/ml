__author__ = 'ssatpati'


import numpy as np
import random

# m denotes the number of examples here, not the number of features
def gradientDescent(x, y, theta, alpha, m, numIterations):
    print "[x] {0}\n{1}\n".format(x.shape, x[:2])
    print "[theta] {0}\n{1}\n".format(theta.shape, theta[:2])
    print "[y] {0}\n{1}\n".format(y.shape, y[:2])
    xTrans = x.transpose()
    for i in range(0, numIterations):
        hypothesis = np.dot(x, theta)
        print "[hypothesis] {0}\n{1}\n".format(hypothesis.shape, hypothesis[:2])
        loss = hypothesis - y
        print "[loss] {0}\n{1}\n".format(loss.shape, loss[:2])
        print "[xTrans] {0}\n{1}\n".format(xTrans.shape, xTrans[:2])
        # avg cost per example (the 2 in 2*m doesn't really matter here.
        # But to be consistent with the gradient, I include it)
        cost = np.sum(loss ** 2) / (2 * m)
        print("\nIteration %d | Cost: %f" % (i, cost))
        # avg gradient per example
        gradient = np.dot(xTrans, loss) / m
        # update
        theta = theta - alpha * gradient
        if i == 0:
            break
    return theta


def genData(numPoints, bias, variance):
    x = np.zeros(shape=(numPoints, 2))
    y = np.zeros(shape=numPoints)
    # basically a straight line
    for i in range(0, numPoints):
        # bias feature
        x[i][0] = 1
        x[i][1] = i
        # our target variable
        y[i] = (i + bias) + random.uniform(0, 1) * variance
    return x, y

# gen 100 points with a bias of 25 and 10 variance as a bit of noise
x, y = genData(100, 25, 10)
m, n = np.shape(x)
numIterations= 100000
alpha = 0.0005
theta = np.ones(n)
theta = gradientDescent(x, y, theta, alpha, m, numIterations)
print(theta)