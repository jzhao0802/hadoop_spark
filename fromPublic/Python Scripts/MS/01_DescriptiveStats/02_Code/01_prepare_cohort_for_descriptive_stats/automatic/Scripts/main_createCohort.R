rm(list=ls())
library(plyr)
library(dplyr)
library(caret)
# 001 create cohort table

# 
main.wkDir <- "./"
setwd(main.wkDir)
source("./Funs/funs_creatCohort.R")
# main.inDir <- paste0(main.wkDir, '../01_Data/')
main.inDir <- "F:\\Jie\\MS\\01_Data\\"

main.timeStamp <- as.character(Sys.time())
main.timeStamp <- gsub(":", ".", main.timeStamp)  # replace ":" by "."
main.outDir <- paste("./03_CohortData/",  main.timeStamp, "/", sep = '')
dir.create(main.outDir, showWarnings = TRUE, recursive = TRUE, mode = "0777")
main.bTransf <- T
main.bTest <- F
main.inFileNm <- "MS_decsupp_analset_20160701"
main.inFileExt <- ".csv"

main.cohortLst <- 1:5

main.outcomeLst <- c("edssprog"
                     , 'edssconf3'
                     , 'relapse_fu_any_01'
                     , 'relapse_or_prog'
                     , 'relapse_and_prog'
                     , 'relapse_or_conf')

main.na_represents <- c('', 'NA', 'unknown', 'ambiguous')

main.varDefCati <- c('gender', 'birth_region')

main.threshold4merge <- 0.1

# main.seedLst <- c(20, 123, 1234, 12345, 123456)
main.seedLst <- c(20)

outDir <- main.outDir
inDir <- main.inDir
cohortLst <- main.cohortLst
outcomeLst <- main.outcomeLst
bCati <- main.bCati
bTest <- main.bTest
inFileNm <- main.inFileNm
inFileExt <- main.inFileExt
na_represents <- main.na_represents
varDefCati <- main.varDefCati
threshold <- main.threshold4merge


for(seed in main.seedLst){
    temp_re1 <- createCohortTb(inDir=main.inDir
                              , inFileNm=main.inFileNm
                              , inFileExt=main.inFileExt
                              , outDir=main.outDir
                              , cohortLst=main.cohortLst
                              , outcomeLst=main.outcomeLst
                              , bTransf=F
                              , na_represents=main.na_represents
                              , varDefCati=main.varDefCati
                              , threshold=main.threshold4merge
                              , bTest=main.bTest
                              , bQcMode = T
                              , seed=seed)
    
    temp_re2 <- createCohortTb(inDir=main.inDir
                              , inFileNm=main.inFileNm
                              , inFileExt=main.inFileExt
                              , outDir=main.outDir
                              , cohortLst=main.cohortLst
                              , outcomeLst=main.outcomeLst
                              , bTransf=T
                              , na_represents=main.na_represents
                              , varDefCati=main.varDefCati
                              , threshold=main.threshold4merge
                              , bTest=main.bTest
                              , bQcMode = T
                              , seed = seed)
}






