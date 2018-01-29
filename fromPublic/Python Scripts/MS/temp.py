# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import time as tm
import datetime as dtm
import os



mainInDir = "C:/Users/jzhao/Documents/Python Scripts/MS/01_DescriptiveStats/01_Data/"
mainTimeStamp = dtm.datetime.fromtimestamp(tm.time()).strftime('%Y.%m.%d.%H.%M.%S')
mainOutDir = "./Results/" + mainTimeStamp + "/"
ensureDir(mainOutDir)

def ensureDir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)
        
