# -*- coding: utf-8 -*-
__author__ = 'hjin'

# Created by Hui on 21/10/2016
#1.using GridSearchCV or RandomizedSearchCV to find the best parameters
#2.calculate the test AUC and AUPR
#3.cross validate with Spark

#Modified by Jie Zhao on 01Nov
for name in dir():
    if not name.startswith('_'):
        del globals()[name]

import pandas as pd
import numpy as np

from time import time
from operator import itemgetter
import scipy.stats as sp_stat

from sklearn.grid_search import GridSearchCV
from sklearn.cross_validation import PredefinedSplit
from sklearn.metrics import roc_auc_score, precision_recall_curve, auc
from sklearn.ensemble import RandomForestClassifier

import datetime
import os
import multiprocessing as mp

import sys
import time as tm
import datetime as dtm

def ensureDir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)
        
mainTimeStamp = dtm.datetime.fromtimestamp(tm.time()).strftime('%Y.%m.%d.%H.%M.%S')
mainOutDir = "./Results/" + mainTimeStamp + "/"
ensureDir(mainOutDir)
#os.chdir(mainOutDir)

#abspath = os.path.abspath(sys.argv[0])
#dname = os.path.dirname(abspath)
realpath = os.path.dirname(os.path.realpath(sys.argv[0]))
os.chdir(realpath)

mainInPath = './Data/'
mainInFileNm = 'data_LRRFTemplat_FoldID.csv'

inPath = mainInPath
inFileNm = mainInFileNm
outPath = mainOutDir

def main(inPath, inFileNm, outPath):
    # user to specify: hyper-params
    numtree = [20, 30]
    numdepth = [3, 4]
    nodesize = [3, 5]
    mtry = ["auto", 2]

    # user to specify : seed in Random Forest model
    iseed = 42

    # user to specify: input data location
    dataFileName = mainInPath + mainInFileNm

    # read data
    data = pd.read_csv(dataFileName)
    orgPredictorCols = data.columns[1:-2]

    #output folder
    outputpath = outPath

    #generate the Random Forest model
    rf = RandomForestClassifier(random_state =iseed)

    #build the grid-search
    paramGrid = dict(n_estimators = numtree, max_depth = numdepth,
                     min_samples_leaf = nodesize, max_features = mtry)

    predictionsAllData = None
    lableAllData =None

    #outer loops for cross-evaluation
    for oFold in range(3):

        #select training and test data
        ocondition = data.OuterFoldID == oFold
        testData = data[(ocondition)]
        trainData = data[~(ocondition)]

        #split model matrix and label y
        tr_x = trainData.ix[:, 1:-2]
        tr_y = trainData.ix[:, 0]
        ts_x = testData.ix[:, 1:-2]
        ts_y = testData.ix[:, 0]

        #using predefined inner cross-validation loop indices
        ps = PredefinedSplit(test_fold=list(trainData.InnerFoldID))

        #build the grid-search cross-validation model
        clf_cv = GridSearchCV(estimator =rf,
                              param_grid=paramGrid,
                              scoring='roc_auc',
                              cv= ps,
                              refit=True,
                              n_jobs=1)

        #fit the model
        cv_model = clf_cv.fit(tr_x, tr_y)

        #get the best model
        best_model = cv_model.best_estimator_

        #predicted on test data with best parameters
        predictions = best_model.predict_proba(ts_x)

        #save the predictions and true label
        if predictionsAllData is not None:
            predictionsAllData = np.concatenate((predictionsAllData,predictions), axis=0)
            lableAllData = pd.concat([lableAllData, ts_y], axis=0)
        else:
            predictionsAllData = predictions
            lableAllData = ts_y

        # save the hyper-parameters of the best model
        sel_parm = cv_model.best_params_
        with open(outputpath + "bestParamsFold" + str(oFold) + ".txt",
                  "w") as fileBestParams:
            fileBestParams.write(str(sel_parm))

        # save importance score of the best model
        f_imp = best_model.feature_importances_
        with open(outputpath + "importanceScoreFold" + str(oFold) + ".txt",
                  "w") as filecvCoef:
            for id in range(len(orgPredictorCols)):
                filecvCoef.write("%s : %f" %(orgPredictorCols[id], f_imp[id]))
                filecvCoef.write("\n")


    #output the predicted scores
    pred_score = np.column_stack((lableAllData.values, predictionsAllData[:,1]))
    column = np.array([['true_label', 'prob']])
    final_out = np.concatenate((column, pred_score), axis=0)
    np.savetxt((outputpath + '/pred_score.csv'), final_out, fmt='%s',
           delimiter=',')

    #calculate AUC and AUPR
    ts_auc = roc_auc_score(lableAllData.values, predictionsAllData[:, 1])
    precision, recall, thresholds = precision_recall_curve(lableAllData.values, predictionsAllData[:, 1])
    ts_aupr = auc(recall, precision)

    #output AUC and AUPR
    with open(outputpath + "auc_aupr.txt", "w") as filePerf:
        filePerf.write("AUC: {}".format(ts_auc))
        filePerf.write('\n')
        filePerf.write("AUPR: {}".format(ts_aupr))

if __name__ == '__main__':

    main(inPath=mainInPath, inFileNm=mainInFileNm, outPath=mainOutDir)



