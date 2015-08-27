__author__ = 'ssatpati'

import pandas as pd
from pandas.tools.plotting import scatter_matrix
import statsmodels.api as sm
import pylab as pl
import numpy as np
import matplotlib.pyplot as plt

from sklearn import linear_model
from sklearn import preprocessing
from sklearn import svm
from sklearn.ensemble import ExtraTreesClassifier
from sklearn import cross_validation
from sklearn import ensemble

import os

# Read from File
TRAIN = None
KAGGLE_TEST = None

# After Feature Extraction
TRAIN_FX = None
KAGGLE_TEST_FX = None

# After Feature Selection

def peakhour(row):
    if row['workingday'] == 1 and (7 <= row['hour'] <= 9 or 16 <= row['hour'] <= 20):
        return 1
    else:
        return 0

def feature_extraction(df):
    # Copy the DataFrame (TODO)
    dataframe = df.copy(deep=True)
    '''Engineer Features from Data'''
    '''Break Date Time into multiple features: year, month, day, hour etc'''
    dataframe.index = pd.to_datetime(dataframe['datetime']) # creating an index from the timestamp
    dataframe['year']= dataframe.index.year # year
    dataframe['month'] = dataframe.index.month # month
    dataframe['hour'] = dataframe.index.hour # hours
    dataframe['day'] = dataframe.index.dayofweek # day of week (Monday=0, Sunday=6)
    dataframe['dayofyear'] = dataframe.index.dayofyear
    dataframe['week'] = dataframe.index.week
    dataframe['quarter'] = dataframe.index.quarter

    # Weather
    #weather_dummies = pd.get_dummies(dataframe['weather'], prefix='weather')
    #dataframe = pd.concat([dataframe, weather_dummies], axis=1)

    dataframe['weather1'] = dataframe['weather'].map(lambda x: 1 if x == 1 else 0)
    dataframe['weather2'] = dataframe['weather'].map(lambda x: 1 if x == 2 else 0)
    dataframe['weather3'] = dataframe['weather'].map(lambda x: 1 if x == 3 else 0)
    dataframe['weather4'] = dataframe['weather'].map(lambda x: 1 if x == 4 else 0)

    # Season
    #season_dummies = pd.get_dummies(dataframe['season'], prefix='season')
    #dataframe = pd.concat([dataframe, season_dummies], axis=1)

    dataframe['season1'] = dataframe['season'].map(lambda x: 1 if x == 1 else 0)
    dataframe['season2'] = dataframe['season'].map(lambda x: 1 if x == 2 else 0)
    dataframe['season3'] = dataframe['season'].map(lambda x: 1 if x == 3 else 0)
    dataframe['season4'] = dataframe['season'].map(lambda x: 1 if x == 4 else 0)

    # Sunday (Registered: Least # of Bikes Rented on Sundays)
    dataframe['sunday'] = dataframe['day'].map(lambda x: 1 if x == 6 else 0)
     # Sunday (Registered: Highest # of Bikes Rented on Sundays)
    dataframe['saturday'] = dataframe['day'].map(lambda x: 1 if x == 5 else 0)

    # Bucket Hours of Day
    #labels = ['0-3', '4-7', '8-11', '12-15', '16-19', '20-23']
    #lens['age_group'] = pd.cut(dataframe.hour, range(0, 23, 6), right=False, labels=labels)
    dataframe['hour_0_3'] = dataframe['hour'].map(lambda x: 1 if 0 <= x <= 3 else 0)
    dataframe['hour_4_7'] = dataframe['hour'].map(lambda x: 1 if 4 <= x <= 7 else 0)
    dataframe['hour_8_11'] = dataframe['hour'].map(lambda x: 1 if 8 <= x <= 11 else 0)
    dataframe['hour_12_15'] = dataframe['hour'].map(lambda x: 1 if 12 <= x <= 15 else 0)
    dataframe['hour_16_19'] = dataframe['hour'].map(lambda x: 1 if 16 <= x <= 19 else 0)
    dataframe['hour_20_23'] = dataframe['hour'].map(lambda x: 1 if 20 <= x <= 23 else 0)

    # Peak Hours (17 & 18)
    dataframe['peakhours'] = dataframe.apply(peakhour, axis=1)

    # Year
    dataframe['year_2011'] = dataframe['year'].map(lambda x: 1 if x == 2011 else 0)
    dataframe['year_2012'] = dataframe['year'].map(lambda x: 1 if x == 2012 else 0)

    return dataframe


def feature_selection(training, kaggle_test=KAGGLE_TEST):
    '''Select Features from Data'''
    '''
    features = ['season', 'holiday', 'workingday', 'weather', 'temp', 'atemp', 'humidity', 'windspeed',
                'year', 'month', 'hour', 'day', 'dayofyear', 'week', 'quarter',
                'weather1', 'weather2', 'weather3', 'weather4',
                'season1', 'season2', 'season3', 'season4', 'sunday',
                'hour_0_3', 'hour_4_7', 'hour_8_11', 'hour_12_15', 'hour_16_19', 'hour_20_23']


    features = ['season', 'holiday', 'workingday', 'weather', 'temp', 'atemp', 'humidity', 'windspeed',
                'year', 'month', 'day', 'dayofyear', 'week',
                'weather1', 'weather2', 'weather3', 'weather4',
                'season1', 'season2', 'season3', 'season4', 'sunday',
                'hour_0_3', 'hour_4_7', 'hour_8_11', 'hour_12_15', 'hour_16_19', 'hour_20_23']
    '''
    # Separate Features for Count, Registered & Casual
    features = ['season', 'holiday', 'workingday', 'weather', 'temp', 'atemp', 'humidity', 'windspeed',
                'month', 'hour', 'day', 'dayofyear', 'week', 'quarter',
                'year_2011', 'year_2012', 'hour_16_19']

    features_r = ['season', 'workingday', 'temp', 'atemp', 'humidity', 'windspeed',
                  'weather1', 'weather2', 'weather3', 'weather4',
                'month', 'hour', 'day', 'dayofyear',
                'year', 'sunday', 'peakhours']

    features_c = ['season', 'holiday', 'workingday', 'temp', 'atemp', 'humidity', 'windspeed',
                  'weather1', 'weather2', 'weather3', 'weather4',
                'month', 'hour', 'day', 'dayofyear',
                'year', 'saturday']

    # Kaggle
    #kaggle_test_set = kaggle_test[features]

    # Training Set
    #features.insert(0, 'count')
    #training_set = training[features]

    # Training Set (Registered)
    #features_r.insert(0, 'registered')
    #training_set_r = training[features_r]

    # Training Set (Casual)
    #features_c.insert(0, 'casual')
    #training_set_c = training[features_c]

    return (features, features_r, features_c)


def inv_log(a):
    return np.exp(a) - 1


##RMSLE Score Function (for Kaggle Evaluation)
def RMSLE_score(Y_pred, Y_act):
    a = (np.log(Y_pred+1)-np.log(Y_act+1))
    b = 1./len(Y_pred)
    score = (b*sum(a**(2)))**(0.5)
    return score


def RMSE_score(log_Y_pred, log_Y_act):
    n = len(log_Y_pred)
    return np.sqrt(1/n*(np.sum((log_Y_pred-log_Y_act)**2)))


def model_summary(estimator, data, labels):
    predictions = estimator.predict(data)

    # The mean square error
    sum_squares = np.mean((np.rint(predictions) - labels) ** 2)
    print("Residual sum of squares: %.2f"
          % sum_squares)
    # Explained variance score: 1 is perfect prediction
    r_squared = estimator.score(data, labels)
    print('[DEV] R^2 - Variance score: %.2f' % r_squared)
    # RMSLE Kaggle
    # root mean square log error
    predictions[predictions < 0] = 0
    rmsle = RMSLE_score(predictions, labels)
    print "RMSLE score: %.6f" % rmsle
    return (sum_squares, r_squared, rmsle)

def output_model_summary(estimator, dev_data, dev_labels, test_data=None, test_labels=None):
    '''Prints Model Summary'''

    # The coefficients
    if 'coef_' in dir(estimator):
        print('Coefficients: \n', estimator.coef_)

    if 'intercept_' in dir(estimator):
        print('Intercept: \n', estimator.intercept_)

    if dev_data is not None and dev_labels is not None:
        model_summary(estimator, dev_data, dev_labels)

    if test_data is not None and test_labels is not None:
        model_summary(estimator, test_data, test_labels)


# Kaggle Baseline
def generate_kaggle_submission(pred, f_name):
    print "\n\n Generating Kaggle Submission File: %s" % (f_name)
    print "Shape of Kaggle Test Set: ", KAGGLE_TEST.shape
    print "Shape of Kaggle Test Set Prediction: ", pred.shape
    print pred
    pred = np.rint(pred)
    pred = np.where(pred < 0, 0, pred)
    df_pred = pd.DataFrame(pred, columns=['count'])

    df_dt = pd.DataFrame(KAGGLE_TEST['datetime'])
    df_dt.reset_index(drop=True, inplace=True)

    print df_dt.head()
    print df_pred.head()

    output = pd.concat([df_dt, df_pred], axis=1)
    print "Shape of Submission Dataframe: ", output.shape
    print output.head()

    file = [os.getcwd(),'/submissions/',f_name]
    output.to_csv("".join(file), index=False)


def feature_engineering_common(Y, X, X1):
    print "### Shape of training set (X)", X.shape
    print "### Shape of labels (Y)", Y.shape
    print "### Shape of Kaggle Test set (X1)", X1.shape

    # Scale features
    scaler = preprocessing.StandardScaler()
    X_SCALED = scaler.fit_transform(X)
    X1_SCALED = scaler.transform(X1)
    print "### (After scaling) Shape of training set", X_SCALED.shape
    print "### (After scaling ) Shape of Kaggle Test set", X1_SCALED.shape

    # Find Important Features using Random Forest
    xtClf = ExtraTreesClassifier().fit(X_SCALED, Y)
    X_SCALED_SUBSET = xtClf.transform(X_SCALED)
    X1_SCALED_SUBSET = xtClf.transform(X1_SCALED)
    importances = xtClf.feature_importances_
    print xtClf.feature_importances_
    print "### (After scaling & feature selection using Random Forrest) Shape of training set", X_SCALED_SUBSET.shape
    print "### (After scaling & feature selection using Random Forrest) Shape of Kaggle Test set", X1_SCALED_SUBSET.shape

    indices = np.argsort(importances)[::-1]

    # Print the feature ranking
    print("Feature ranking:")
    for f in xrange(10):
        print("%d. feature %d (%f)" % (f + 1, indices[f], importances[indices[f]]))


def feature_engineering_registered(features_r):
    TRAIN_FX_FS = TRAIN_FX[features_r]
    KAGGLE_TEST_FX_FS = KAGGLE_TEST_FX[features_r]
    Y = TRAIN_FX['registered'].values
    X = TRAIN_FX_FS.values
    X1 = KAGGLE_TEST_FX_FS.values
    feature_engineering_common(Y, X, X1)


def feature_engineering_casual(features_c):
    TRAIN_FX_FS = TRAIN_FX[features_c]
    KAGGLE_TEST_FX_FS = KAGGLE_TEST_FX[features_c]
    Y = TRAIN_FX['casual'].values
    X = TRAIN_FX_FS.values
    X1 = KAGGLE_TEST_FX_FS.values
    feature_engineering_common(Y, X, X1)


def feature_engineering():
    global TRAIN_FX
    TRAIN_FX = feature_extraction(TRAIN)
    global KAGGLE_TEST_FX
    KAGGLE_TEST_FX = feature_extraction(KAGGLE_TEST)

    (features, features_r, features_c) = feature_selection(TRAIN_FX, KAGGLE_TEST_FX)

    # Common
    print("### Doing Feature Engineering for count ###")
    TRAIN_FX_FS = TRAIN_FX[features]
    KAGGLE_TEST_FX_FS = KAGGLE_TEST_FX[features]
    Y = TRAIN_FX['count'].values
    X = TRAIN_FX_FS.values
    X1 = KAGGLE_TEST_FX_FS.values
    feature_engineering_common(Y, X, X1)
    print("----------------------------------------------")

    #Registered
    print("### Doing Feature Engineering for registered ###")
    feature_engineering_registered(features_r)
    print("----------------------------------------------")

    #Casual
    print("### Doing Feature Engineering for registered ###")
    feature_engineering_casual(features_c)
    print("----------------------------------------------")


def model(TRAIN_FX, KAGGLE_TEST_FX):
    # Feature Selection & Scaling
    TRAIN_FX_FS, KAGGLE_TEST_FX_FS  = feature_selection(TRAIN_FX, KAGGLE_TEST_FX)
    Y = TRAIN_FX_FS['count'].values
    X = TRAIN_FX_FS.drop('count', axis=1).values
    X1 = KAGGLE_TEST_FX_FS.values
    print "### Shape of training set (X)", X.shape
    print "### Shape of labels (Y)", Y.shape
    print "### Shape of Kaggle Test set (X1)", X1.shape

    # Scale features
    scaler = preprocessing.StandardScaler()
    X_SCALED = scaler.fit_transform(X)
    X1_SCALED = scaler.transform(X1)
    print "### (After scaling) Shape of training set", X_SCALED.shape
    print "### (After scaling ) Shape of Kaggle Test set", X1_SCALED.shape

    # Find Important Features using Random Forest
    xtClf = ExtraTreesClassifier().fit(X_SCALED, Y)
    X_SCALED_SUBSET = xtClf.transform(X_SCALED)
    X1_SCALED_SUBSET = xtClf.transform(X1_SCALED)
    importances = xtClf.feature_importances_
    print xtClf.feature_importances_
    print "### (After scaling & feature selection using Random Forrest) Shape of training set", X_SCALED_SUBSET.shape
    print "### (After scaling & feature selection using Random Forrest) Shape of Kaggle Test set", X1_SCALED_SUBSET.shape

    indices = np.argsort(importances)[::-1]

    # Print the feature ranking
    print("Feature ranking:")
    for f in xrange(10):
        print("%d. feature %d (%f)" % (f + 1, indices[f], importances[indices[f]]))

    #  Random Forrest with Cross Validation
    rf = ensemble.RandomForestRegressor(n_estimators=100)
    ss = cross_validation.ShuffleSplit(X.shape[0], n_iter=5, test_size=0.25, random_state=0)
    count = 1
    for train_index, test_index in ss:
        print("\n [Iteration:%d] Num of Training: %s,  Num of Test: %s" % (count, len(train_index), len(test_index)))
        # Train the model using the training sets
        rf.fit(X[train_index], Y[train_index])

        # Model Summary
        output_model_summary(rf, X[test_index], Y[test_index])

        count += 1

    # Train the model using the entire data set
    rf.fit(X, Y)
    pred = rf.predict(X1)
    generate_kaggle_submission(pred, "rf.csv")


def model_reg_cas():
    global TRAIN_FX
    TRAIN_FX = feature_extraction(TRAIN)

    global KAGGLE_TEST_FX
    KAGGLE_TEST_FX = feature_extraction(KAGGLE_TEST)

    (features, features_r, features_c) = feature_selection(TRAIN_FX, KAGGLE_TEST_FX)

    Y_COUNT = TRAIN_FX['count'].values

    TRAIN_FX_FS_R = TRAIN_FX[features_r]
    KAGGLE_TEST_FX_FS_R = KAGGLE_TEST_FX[features_r]
    Y_R = TRAIN_FX['registered'].values
    X_R = TRAIN_FX_FS_R.values
    X1_R = KAGGLE_TEST_FX_FS_R.values

    TRAIN_FX_FS_C = TRAIN_FX[features_c]
    KAGGLE_TEST_FX_FS_C = KAGGLE_TEST_FX[features_c]
    Y_C = TRAIN_FX['casual'].values
    X_C = TRAIN_FX_FS_C.values
    X1_C = KAGGLE_TEST_FX_FS_C.values

    #  Random Forrest with Cross Validation
    rf_reg = ensemble.RandomForestRegressor(n_estimators=100)
    rf_cas = ensemble.RandomForestRegressor(n_estimators=100)
    ss = cross_validation.ShuffleSplit(X_R.shape[0], n_iter=5, test_size=0.25, random_state=0)
    count = 1
    for train_index, test_index in ss:
        print("\n [Iteration:%d] Num of Training: %s,  Num of Test: %s" % (count, len(train_index), len(test_index)))

        # Train the model using the training sets
        rf_reg.fit(X_R[train_index], Y_R[train_index])

         # Train the model using the training sets
        rf_cas.fit(X_C[train_index], Y_C[train_index])


        pred_reg = rf_reg.predict(X_R[test_index])
        pred_cas = rf_cas.predict(X_C[test_index])

        predictions = pred_reg + pred_cas

        # The mean square error
        sum_squares = np.mean((np.rint(predictions) - Y_COUNT[test_index]) ** 2)
        print("Residual sum of squares: %.2f"
              % sum_squares)
        # Explained variance score: 1 is perfect prediction
        r_squared = (rf_reg.score(X_R[test_index], Y_R[test_index]) + rf_cas.score(X_C[test_index], Y_C[test_index])) / 2
        print('[DEV] R^2 - Variance score: %.2f' % r_squared)
        # RMSLE Kaggle
        # root mean square log error
        predictions[predictions < 0] = 0
        rmsle = RMSLE_score(predictions, Y_COUNT[test_index])
        print "RMSLE score: %.6f" % rmsle

        count += 1

    feature_importances__reg = rf_reg.feature_importances_
    print feature_importances__reg
    indices = np.argsort(feature_importances__reg)[::-1]

    # Print the feature ranking
    print("[Registered] Feature ranking:")
    for f in xrange(len(features_r)):
        print("%d. Feature %d - %s : (%f)" % (f + 1, indices[f],  features_r[indices[f]], feature_importances__reg[indices[f]]))

    feature_importances__cas = rf_cas.feature_importances_
    print feature_importances__cas
    indices = np.argsort(feature_importances__cas)[::-1]

    # Print the feature ranking
    print("[Casual] Feature ranking:")
    for f in xrange(len(features_c)):
        print("%d. Feature %d - %s : (%f)" % (f + 1, indices[f],  features_c[indices[f]], feature_importances__cas[indices[f]]))

    # Train the model using the entire data set
    rf_reg.fit(X_R, Y_R)
    rf_cas.fit(X_C, Y_C)
    predictions = rf_reg.predict(X1_R) + rf_cas.predict(X1_C)
    generate_kaggle_submission(predictions, "rf.csv")

if __name__ == '__main__':
    TRAIN = pd.read_csv("train.csv")
    KAGGLE_TEST = pd.read_csv("test.csv")


    #feature_engineering()
    model_reg_cas()

    '''
    # Perform Feature Extraction
    TRAIN_FX = feature_extraction(TRAIN)
    KAGGLE_TEST_FX = feature_extraction(KAGGLE_TEST)

    #Model
    model(TRAIN_FX, KAGGLE_TEST_FX)
    '''

