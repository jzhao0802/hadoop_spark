# -*- coding: utf-8 -*-
"""
Created on Thu Jun 30 15:21:17 2016

@author: zywang
"""
import sys
import os
import time
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import numpy as np
from pyspark.sql import functions as F

# Some parameters
data_path = 's3://emr-rwes-pa-spark-dev-datastore/'
#inpath = 'Jie/MS/Jul05/Cohort_Lichao/'
inpath = 'Jie/MS/Jul12/2016-07-11 20.01.50/'
datapath = data_path + inpath
runType = sys.argv[1]
filenameList = ['B2B',
                'B2Fir',
                'B2Sec',
                'BConti',
                'Cmp']
responseList = ["relapse_fu_any_01",
                "relapse_or_prog",
                "relapse_and_prog",
                "relapse_or_conf",
                "edssprog",
                "edssconf3"]
fileid = int(sys.argv[2])
app_name = "MS_" + runType + "_" + filenameList[fileid]

def data_ds(data):    
    feature = data.columns
    totalcnt = data.count()
    colcnt = len(feature)
    def percentile(colid):
        d = data.select(feature[colid]).na.drop().cache()
        d = d.withColumnRenamed(feature[colid], "var")
        unqueCnt = d.distinct().count()
        d.registerTempTable("d_sql")
        #for the sum, missing and mean values and percentiles
        if (unqueCnt == 1):
            if (d.distinct().collect()[0][0] == ''):
                variable = feature[colid]
                ColumnTypeFlag = 3
                totalCnt = totalcnt
                nonMissing = 0 
                NumberInThisCategory = 0 
                meanValue = ''
            else:
                variable = feature[colid]
                ColumnTypeFlag = 2
                totalCnt = totalcnt
                nonMissing = d.count()                
                a = d.groupby().agg(F.abs(F.sum(F.col("var"))),\
                                F.round(F.avg(F.col("var")),3))\
                            .collect()[0]
                NumberInThisCategory = a[0]                    
                meanValue = a[1] 
            minValue = ''
            percentile1 = ''
            percentile5 = ''
            percentile10 = ''
            percentile25 = ''
            percentile50 = ''
            percentile75 = ''
            percentile90 = ''
            percentile95 = ''
            percentile99 = ''
            maxValue = ''
        elif (unqueCnt == 2): 
            variable = feature[colid]
            ColumnTypeFlag = 1
            totalCnt = totalcnt
            nonMissing = d.count()
            a = d.groupby().agg(F.abs(F.sum(F.col("var"))),\
                                F.round(F.avg(F.col("var")),3))\
                            .collect()[0]
            NumberInThisCategory = a[0]                    
            meanValue = a[1]           
            minValue = ''
            percentile1 = ''
            percentile5 = ''
            percentile10 = ''
            percentile25 = ''
            percentile50 = ''
            percentile75 = ''
            percentile90 = ''
            percentile95 = ''
            percentile99 = ''
            maxValue = ''
        else:
            variable = feature[colid]
            ColumnTypeFlag = 0
            totalCnt = totalcnt
            nonMissing = d.count()
            a = d.groupby().agg(F.abs(F.sum(F.col("var"))),\
                                F.round(F.avg(F.col("var")),3))\
                            .collect()[0]
            NumberInThisCategory = ''                   
            meanValue = a[1]            
            d2 =  d.rdd.sortBy(lambda x: x).zipWithIndex().map(lambda x: (x[1], x[0])).cache()
            subcnt = d2.count()
            minValue = float(d2.lookup(0)[0][0])
            percentile1 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 1)) - 1)[0][0])
            percentile5 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 5)) - 1)[0][0])
            percentile10 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 10)) - 1)[0][0])
            percentile25 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 25)) - 1)[0][0])
            percentile50 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 50)) - 1)[0][0])
            percentile75 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 75)) - 1)[0][0])
            percentile90 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 90)) - 1)[0][0])
            percentile95 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 95)) - 1)[0][0])
            percentile99 = float(d2.lookup(int(np.ceil(subcnt / 100.0 * 99)) - 1)[0][0])
            maxValue = float(d2.lookup(subcnt - 1)[0][0])
        d1List = [variable, ColumnTypeFlag, totalCnt, nonMissing, NumberInThisCategory, meanValue]
        percentile = [minValue, percentile1, percentile5, percentile10,\
                      percentile25, percentile50, percentile75,\
                      percentile90, percentile95, percentile99, maxValue]
        final_list = tuple(d1List +  percentile)
        return(final_list)
    ds_tb1 = [percentile(colid) for colid in range(colcnt)]
    nameList = ["variable","ColumnTypeFlag","totalCnt","nonMissing",\
                "NumberInThisCategory", "meanValue",\
                "minValue", "percentile1", "percentile5","percentile10", \
                "percentile25", "percentile50","percentile75", "percentile90", \
                "percentile95","percentile99", "maxValue"]
    final_tb = sqlContext.createDataFrame(ds_tb1, nameList)  
    final_tb = final_tb.withColumn("MissingCnt", final_tb.totalCnt - final_tb.nonMissing).coalesce(1)
    final_tb.registerTempTable("final_tb_t")
    resultQuery = "select variable,ColumnTypeFlag, totalCnt as NumberOfRecords," + \
                  "MissingCnt as NumberOfMissing, round(MissingCnt/totalCnt,3) as pctOfMissing,"+\
                  "NumberInThisCategory,round(NumberInThisCategory/totalCnt,3) as pctOfThsCategory," + \
                  "round(meanValue,3) as AvgValue, minValue, percentile1, percentile5," +\
                  "percentile10, percentile25, percentile50, percentile75,"+\
                  "percentile90, percentile95, percentile99, maxValue " +\
                  "from final_tb_t"
    com_table_final = sqlContext.sql(resultQuery).coalesce(1)
    return(com_table_final)

def cross_tb(data, responseList):
    head = [x for x in data.columns if x not in responseList] 
    # Select the features less than 3 levels and obtain the table
    def unique_cnt(i):
        temp = data.select(head[i]).na.drop().distinct().count()
        if (temp == 2):
            return(head[i])    
    fe = [unique_cnt(i) for i in range(len(head))]
    feature = [x for x in fe if x is not None]     
    #Calculate the cross table between features and response    
    def allCS(resid):
        response = responseList[resid]
        tcnt = data.groupBy().sum(response).collect()[0][0]
        def crssTb(i):
            cs_tb = data.stat.crosstab(feature[i], response)\
                    .withColumnRenamed(feature[i] + '_' + response,'value')
            cs_tb1 =  cs_tb.filter(cs_tb.value != 0).collect()       
            variable = feature[i]        
            response0 = cs_tb1[0][1]
            response1 = cs_tb1[0][2]
            NegPct = round(float(response0)/float(response0 + response1),3)
            PosPct = round(float(response1)/float(response0 + response1),3)
            posRate = round(float(response1)/float(tcnt),3)
            reList = (variable, response0, NegPct, response1,PosPct, posRate)
            return(reList)
        cs_result = [crssTb(i) for i in range(len(feature))]  
        name = ["variable", response + "_Neg", response + "_NegPct",\
                response + "_Pos", response + "_PosPct",response + "_PosRate"]
        cs_result_F = sqlContext.createDataFrame(cs_result, name).coalesce(1)
        return(cs_result_F) 
    allCS_list = [allCS(resid) for resid in range(len(responseList))]   
    allCS = allCS_list[0]
    for lsid in range(1,len(allCS_list)):
        allCS = allCS.join(allCS_list[lsid], allCS.variable == allCS_list[lsid].variable,\
                           how = "inner").drop(allCS_list[lsid].variable).cache()
    allCS = allCS.coalesce(1)
    return(allCS)

if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName(app_name)
    # set the dynamic executors
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.shuffle.service.enabled", "true")
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)
    #Create the results target folder
    start_time = time.time()
#    st = datetime.datetime.fromtimestamp(start_time).strftime('%Y%m%d_%H%M%S')  
    st = datetime.datetime.fromtimestamp(start_time).strftime('%Y%m%d')  
#    resultDir_s3 = datapath + runType + "_" + st + "/"
    resultDir_s3 = datapath + 'Jie/MS/Jul19/' + runType + "_" + st + "/"
    if not os.path.exists(resultDir_s3):
        os.makedirs(resultDir_s3)
    
    filename = filenameList[fileid]
    #Reading in data as RDD data
    data = sqlContext.read.load(datapath + filename + ".csv", 
                                format='com.databricks.spark.csv', 
                                header='true', 
                                inferSchema='true')
    #Rename the data column names
    for strid in range(len(data.columns)):
        oldname = data.columns[strid]
        if (oldname.find(" ") > 0 or oldname.find(">") > 0 or\
            oldname.find("<") > 0 or oldname.find("-") > 0 or\
            oldname.find("[") > 0 or oldname.find("(") > 0 or\
            oldname.find("]") > 0 or oldname.find(")") > 0 or\
	    oldname.find(".") >0):
            newname = oldname.replace(" ", "_")\
                             .replace(">", "GT")\
                             .replace("<", "LT")\
                             .replace("-", "To")\
                             .replace("[", "F")\
                             .replace("(", "F")\
                             .replace("]", "T")\
                             .replace(")", "T")\
			     .replace(".", "dot")
            data = data.withColumnRenamed(oldname, newname)
        #Run the functions
    if (runType.upper() == "DS"):
        com_table_final = data_ds(data = data)
        com_table_final.save(resultDir_s3 + runType + "_" + filename,\
                             "com.databricks.spark.csv",header="true")
    elif(runType.upper() == "CS"):
        cs_table = cross_tb(data = data, responseList = responseList)
        cs_table.save(resultDir_s3 + runType+ "_" + filename,\
                      "com.databricks.spark.csv",header="true")





