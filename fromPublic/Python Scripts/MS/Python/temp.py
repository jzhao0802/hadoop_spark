# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import time as tm
import datetime as dtm
import os
import sys
import pandas as pd
from collections import Counter
import collections
import random as rdm
import numpy as np
from compiler.ast import flatten

def ensureDir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)
        

mainInfileNm = "MS_decsupp_analset_20160701"
mainInfileExt = ".csv"
mainCht = "BConti"
mainChtIdx = 4
mainOutcome = "edssprog"
mainNARepresents = ["", "NA", 'unknown', 'ambiguous']
mainBTest = True
mainSeed = 20
mainBQcMode = True
mainInDir = "C:/Users/jzhao/Documents/Python Scripts/MS/01_DescriptiveStats/01_Data/"
mainTimeStamp = dtm.datetime.fromtimestamp(tm.time()).strftime('%Y.%m.%d.%H.%M.%S')
mainOutDir = "./Results/" + mainTimeStamp + "/"
ensureDir(mainOutDir)

abspath = os.path.abspath(sys.argv[0])
#dname = os.path.dirname(abspath)
os.chdir(abspath)

infileNm = mainInfileNm
infileExt = mainInfileExt
cht = mainCht
chtIdx = mainChtIdx
outcome = mainOutcome
NARepresents = mainNARepresents
bTest = mainBTest
bQcMode = mainBQcMode
inDir = mainInDir
timeStamp = mainTimeStamp
outDir = mainOutDir
seed = mainSeed

#data readin
dt = pd.read_csv(inDir+infileNm+infileExt, sep=',', na_values=NARepresents)
#6112 rows x 426 columns
#add record number
dt['record_num'] = list(range(1, dt.shape[0]+1))
if bTest==True:
    dt = dt.iloc[0:1000, :]
    
dt.columns = dt.columns.str.lower()

#for a certain cohort, for those duplicated ptid, randomly select one line
dt = dt[dt['tblcoh']==chtIdx]

frq = Counter(dt.tblcoh)

rdm.seed(seed)

vars2drop = [['idx_dt'], ['firstdt'], ['idxyr'], ['last_from_dt'], ['has2edss_conf3'], [s for s in dt.columns if "pegint" in s]]

dt.drop(['idx_dt', 'firstdt', 'idxyr', 'last_from_dt', 'has2edss_conf3'], axis=1, inplace=True)

dt.drop([s for s in dt.columns if "pegint" in s], axis=1, inplace=True)

[item for sublist in vars2drop for item in sublist]

np.array(vars2drop)

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
            return flatten(el)
        else:
            return el
            
flatten(vars2drop)

size = 1        # sample size
replace = True  # with replacement
fn = lambda obj: obj.loc[np.random.choice(obj.index, size, replace),:]
dt = dt.groupby('new_pat_id', as_index=False).apply(fn)

dt.drop(['tblcoh'], axis=1, inplace=True)

varLst = np.array(dt.columns)

varsIgn = np.array(["new_pat_id", "record_num"])

if bQcMode == True:
    if dt.shape[0] != dt.new_pat_id.unique().__len__():
        sys.exit("Error message:unique patient id select wrong!\n\n")

cht = np.where(chtIdx==4, "BConti", np.where(chtIdx==1, "B2Sec", "Others"))

#calculate pre index dmts in different years











