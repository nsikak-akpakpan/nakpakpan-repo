from numpy import log, log10, arange, fill_diagonal, ndarray, array, arange, isnan, logical_or
from pyspark import SparkContext, SparkConf, StorageLevel, RDD
from pyspark.sql import SQLContext, Row, HiveContext
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import functions as psf
from pyspark.ml.feature import StringIndexer, VectorSlicer, VectorAssembler
from pyspark.ml.param import Param, Params
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
from pyspark.mllib.stat import KernelDensity

#import sys
#inP = sys.argv[1:]
#if(len(inP) == 12):
#    miPath       = inP[0]
#    scPath       = inP[1]
#    outPath      = inP[2]
#    defNumPart   = int(inP[3])
#    minNIR       = float(inP[4])
#    maxNIR       = float(inP[5])
#    minCom       = float(inP[6])
#    maxCom       = float(inP[7])
#    minMargin    = float(inP[8])
#    maxMargin    = float(inP[9])
#    NIRBandwidth = float(inP[10])
#    ComBandwidth = float(inP[11])
#else:
#    print("Warning: Incorrect number (" + str(len(inP)) + " out of 12) of parameters, using default. \
#    There should be three table locations - PricingModelInput, PricingSampleForVisuals and OutputTable - and numPartitions, \
#    minNIR, maxNIR, minCom, maxCom, minMargin, maxMargin, NIRBandwidth, ComBandwidth")
#    miPath       = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/var/PricingModelInput_parquet"
#    scPath       = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/var/PricingSampleForVisuals_parquet"
#    outPath      = "hdfs://10.82.187.10:8020/hadoop/hdfs/OUT/PricesForVisuals_Camp_26-42-43_bnd50_NIR_5-10_m7.5_parquet"
#    defNumPart   = 36
#    minNIR       = 0.05     # Minimum interest rate allowable
#    maxNIR       = 0.1      # Maximum interest rate allowable
#    minCom       = 0.0      # Minimum commission allowable
#    maxCom       = 0.2      # Maximum commission allowable
#    minMargin    = 0.075    # Minimum margin allowable
#    maxMargin    = 10.0     # Maximum margin allowable
#    NIRBandwidth = 0.001    # Bandwidth for Kernel Density Estimator
#    ComBandwidth = 0.001    # Bandwidth for Kernel Density Estimator

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--prmin',          nargs = '?', default = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/var/PricingModelInput_parquet", help = 'Path to table var/PricingModelInput')
parser.add_argument('--scrin',          nargs = '?', default = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/var/PricingSampleForVisuals_parquet", help = 'Path to table var/PricingSampleForVisuals')
parser.add_argument('--out',            nargs = '?', default = "hdfs://10.82.187.10:8020/hadoop/hdfs/OUT/PricesForVisuals_Camp_26-42-43_bnd50_NIR_5-10_m7.5_parquet", help = 'Path to output table')
parser.add_argument('--numPartitions',  nargs = '?', default = 36, type = int, help = 'Default number of partitions')
parser.add_argument('--minNIR',         nargs = '?', default = 0.05 , type = float, help = 'Minimum Nominal Interest Rate')
parser.add_argument('--maxNIR',         nargs = '?', default = 0.1  , type = float, help = 'Maximum Nominal Interest Rate')
parser.add_argument('--minCom',         nargs = '?', default = 0.0  , type = float, help = 'Minimum Commission Rate')
parser.add_argument('--maxCom',         nargs = '?', default = 0.2  , type = float, help = 'Maximum Commission Rate')
parser.add_argument('--minMargin',      nargs = '?', default = 0.075, type = float, help = 'Minimum Margin')
parser.add_argument('--maxMargin',      nargs = '?', default = 20.0 , type = float, help = 'Maximum Margin')
parser.add_argument('--NIRBandwidth',   nargs = '?', default = 0.001, type = float, help = 'Kernel Density Estimator bandwidth for Nominal Interest Rate')
parser.add_argument('--ComBandwidth',   nargs = '?', default = 0.001, type = float, help = 'Kernel Density Estimator bandwidth for Commission Rate')

inP = parser.parse_args()

miPath       = inP.prmin
scPath       = inP.scrin
outPath      = inP.out
defNumPart   = inP.numPartitions
minNIR       = inP.minNIR      
maxNIR       = inP.maxNIR      
minCom       = inP.minCom      
maxCom       = inP.maxCom      
minMargin    = inP.minMargin   
maxMargin    = inP.maxMargin   
NIRBandwidth = inP.NIRBandwidth
ComBandwidth = inP.ComBandwidth

# setup SparkContext
conf = SparkConf().setAppName("PricingModel")
conf.set('spark.default.parallelism', str(defNumPart))
conf.set('spark.sql.shuffle.partitions', str(defNumPart))
sc = SparkContext(conf = conf)
hc = HiveContext(sc)
saveToHDFS = True

idVars = ['OfferID']
targetName = 'Target'
notToUseVars = ['CreditApplicationID', 'MaxIncomeClientId', 'IdBankiera', 'IsSuccessful', 'VoluntaryInsuranceContribution', 'InstallmentAmount', 'Commission', 'RSSO', 'EIR', 'EIRSimple', 'TopUpAmount', 'ContractID', 'ProductName']
notNullThreshold = 0.05

#MyDFRaw = hc.read.parquet(miPath).filter('OfferID < 480000').coalesce(defNumPart)
MyDFRaw = hc.read.parquet(miPath).coalesce(defNumPart)

toUseVars = list(MyDFRaw.columns)
for f in notToUseVars:
    toUseVars.remove(f)
MyDFToUse = MyDFRaw.select(toUseVars)

### Define categorical and continuous variables
categoricalVarsPre = []
for dt in MyDFToUse.dtypes:
    if dt[1] == 'string':
        categoricalVarsPre.extend([dt[0]])

continuousVarsPre = MyDFToUse.columns
continuousVarsPre.remove(targetName)
for f in idVars:
    continuousVarsPre.remove(f)
for f in categoricalVarsPre:
    continuousVarsPre.remove(f)

### Find variables which have 
### more than one unique value and
### more than notNullThreshold not NULL values and
### compute averages for NULL insertion
desc = MyDFToUse.describe([targetName] + continuousVarsPre + categoricalVarsPre).take(5)
cntNotNull = desc[0][1:]
# We suppose target doesn't have NULLs. If not then use: cnt = float(MyDFToUse.count())
cnt = float(desc[0][targetName]) 
mins = desc[3][1:]
maxs = desc[4][1:]
# If it has distinct values then min is different from max.
isDistinct = [x[0] != x[1] for x in zip(mins, maxs)]
#cntDistinct = MyDFToUse.select(*[psf.countDistinct(c).alias(c) for c in MyDFToUse.columns]).take(1)[0]
avgs = desc[1]
goodVars = []
for idx, x in enumerate(zip(cntNotNull, isDistinct)):
    if float(x[0])/cnt > notNullThreshold and x[1]:
        goodVars.append(([targetName] + continuousVarsPre + categoricalVarsPre)[idx])
MyDF = MyDFToUse.select(idVars + goodVars)
continuousVars = [x for x in continuousVarsPre if x in goodVars]
categoricalVars = [x for x in categoricalVarsPre if x in goodVars]

#### Divide the initial data into train and test data sets ####
#MyTest = MyDF.sampleBy(targetName, {0: 0.3, 1: 0.3}, seed = 0).persist()
#MyTrain = MyDF.subtract(MyTest).persist()
trgtCnt = MyDF.groupBy(targetName).agg(psf.count(targetName).alias('TargetCnt')).orderBy('TargetCnt').take(2)
oneFrac  = 0.75
zeroFrac = oneFrac*trgtCnt[0][1]/trgtCnt[1][1]
MyTrain = MyDF.sampleBy(targetName, {0: zeroFrac, 1: oneFrac}, seed = 0).cache()
MyTest = MyDF.subtract(MyTrain).cache()

# Fill NA values
fillNADict = {}
for cont in continuousVars:
    fillNADict[cont] = float(avgs[cont])
for cat in categoricalVars:
    fillNADict[cat] = 'unknown'

MyTrain = MyTrain.fillna(fillNADict)
MyTest = MyTest.fillna(fillNADict)

####################################################################################################
# WoE Experimental
cats = MyTrain.select(targetName).distinct().collect()
cat0 = cats[0][0]
cat1 = cats[1][0]
sumTaken = MyTrain.select(targetName).filter(MyTrain[targetName] == cat1).count()
sumNotTaken = MyTrain.select(targetName).filter(MyTrain[targetName] == cat0).count()
const = log(sumNotTaken/sumTaken)

woeDict = {}
for f in categoricalVars:
    Taken = MyTrain.select(f, targetName).filter(MyTrain[targetName] == cat1).groupBy(f).count().withColumnRenamed('count', 'NumTaken')
    NotTaken = MyTrain.select(f, targetName).filter(MyTrain[targetName] == cat0).groupBy(f).count().withColumnRenamed('count', 'NumNotTaken')
    Both = Taken.join(NotTaken, on = f, how = 'outer').fillna(1)

    WoE = Both.withColumn('woe' + f, psf.log( Both['NumTaken'] / Both['NumNotTaken'] ) + const).select(f, 'woe' + f)
    WoE.persist(StorageLevel.MEMORY_ONLY)
    orgVals = [val[0] for val in WoE.select(f).collect()]
    woeVals = [str(val[0]) for val in WoE.select('woe' + f).collect()]
    orgVals.append('unknown')
    woeVals.append('0.0')

    woeDictUpdate = {f : [orgVals, woeVals]}
    woeDict.update(woeDictUpdate)
    MyTrain = MyTrain.replace(woeDict[f][0], woeDict[f][1], f).withColumnRenamed(f, 'woe' + f)
    MyTrain = MyTrain.withColumn('woe' + f, MyTrain['woe' + f].cast('float'))
    MyTest = MyTest.replace(woeDict[f][0], woeDict[f][1], f).withColumnRenamed(f, 'woe' + f)
    MyTest = MyTest.withColumn('woe' + f, MyTest['woe' + f].cast('float'))
WoE.unpersist()

# Fill NA values in WoE - they appear when there is new category in test.
descWoE = MyTrain.describe([targetName] + continuousVars + ['woe' + f for f in categoricalVars]).take(5)
avgsWoE = descWoE[1]
fillWoENADict = {}
for cont in continuousVars:
    fillWoENADict[cont] = float(avgsWoE[cont])
for cat in categoricalVars:
    fillWoENADict['woe' + cat] = float(avgsWoE['woe' + cat])
MyTest = MyTest.fillna(fillWoENADict)

############################# Variable selection #############################
#### First stage: Find correlated variables and remove one from each pair ####
##############################################################################
modelVars = list(idVars)
modelVars.extend([targetName])
modelVars.extend(continuousVars)
modelVars.extend(['woe' + f for f in categoricalVars])

allToOne = VectorAssembler(inputCols = modelVars[2:], outputCol = "features")
assembledRDD = allToOne.transform(MyTrain.select(modelVars[2:])).select("features").rdd.map(lambda line: line[0])
corM = Statistics.corr(assembledRDD)
corM.setflags(write = True)
fill_diagonal(corM, 0.0)

nVar = corM.shape[0]
corToMod = array(corM.tolist()) # This is to ensure that a copy is made
uncorrelatedVars = list(modelVars)
threshold = 0.7
for iter in arange(nVar):
    isCor = sum(abs(corToMod[iter]) > threshold) > 0
    if isCor:
        uncorrelatedVars.remove(modelVars[2 + iter])
        corToMod[:, iter] = 0
        corToMod[iter, :] = 0

############################### Variable selection ################################
#### Use regularized logistic regression to filter out non-relevant variables. ####
###################################################################################
### Prepare train and test sample for variable selection
glmFA = VectorAssembler(inputCols = uncorrelatedVars[2:], outputCol = "features")
ClassData = glmFA.transform(MyTrain).select([idVars[0], targetName, "features"]).cache()
ClassData = ClassData.withColumn(targetName, ClassData[targetName].cast("double"))
ClassTestData = glmFA.transform(MyTest).select([idVars[0], targetName, "features"]).cache()
ClassTestData = ClassTestData.withColumn(targetName, ClassTestData[targetName].cast("double"))

### Prepare cross validation
glmnet = LogisticRegression()
modelEvaluator = BinaryClassificationEvaluator(rawPredictionCol = "probability", labelCol = targetName)
gridGlmnet = ParamGridBuilder().baseOn([glmnet.labelCol, targetName], [glmnet.elasticNetParam, 0.0]).addGrid(glmnet.regParam, [0.0, 0.025, 0.5, 0.1, 0.2, 0.4]).build()
cvGlmnet = CrossValidator(estimator = glmnet, estimatorParamMaps = gridGlmnet, evaluator = modelEvaluator)
GLMNET = cvGlmnet.fit(ClassData)

### Define variables with non-zero coefficients
coef = GLMNET.bestModel.coefficients
idxs = [idx for idx, x in enumerate(coef.toArray()) if not x == 0.0]

### Define function, which extracts variables with non-zero coefficients
glmnetChoose = VectorSlicer(inputCol = 'features', outputCol = 'glmnetFeatures', indices = idxs)
def glmnetSelect(df, vecSlc, withIdxCol = False):
    if withIdxCol:
        return vecSlc.transform(df).select(idVars + [targetName, 'indexed' + targetName, 'glmnetFeatures']).withColumnRenamed('glmnetFeatures', 'features')
    else:
        return vecSlc.transform(df).select(idVars + [targetName, 'glmnetFeatures']).withColumnRenamed('glmnetFeatures', 'features')

### Transform the data
GLMData = glmnetSelect(ClassData, glmnetChoose).cache()
GLMTestData = glmnetSelect(ClassTestData, glmnetChoose).cache()
ClassData.unpersist()
ClassTestData.unpersist()

######################## Logistic regression ########################
#### Train a logistic regression model on pre-selected variables ####
#####################################################################
# train the model
glm = LogisticRegression(labelCol = "Target", regParam = 0.0)
model = glm.fit(GLMData)

#Print coefficients
finalVars = ['Intercept']
finalVars.extend([uncorrelatedVars[2:][idx] for idx in idxs])
finalCoefs = [model.intercept]
finalCoefs.extend(model.coefficients.toArray())
coefMap = [(x[0], float(x[1])) for x in zip(finalVars, finalCoefs)]
coefDF = hc.createDataFrame(coefMap, schema = ['Variable', 'Coefficient'])#.coalesce(1)
coefDF.show(len(idxs), truncate = False)

# score the sample
trainProbs = model.transform(GLMData)
testProbs = model.transform(GLMTestData)

# test the model performance - AUC
trainGini = 2*modelEvaluator.evaluate(trainProbs) - 1
print('Model has train Gini ' + str(round(trainGini*100, 2)) + ' %.' )

testGini = 2*modelEvaluator.evaluate(testProbs) - 1
print('Model has test Gini ' + str(round(testGini*100, 2)) + ' %.' )

GLMData.unpersist()
GLMTestData.unpersist()

#print('Time elapsed: ' + str(round(clock() - programStart, 3)) + ' seconds.')

########################### Home-made Kernel Density Estimator ###########################
####     There is no two-dimensional KDE. In this approach moving average is used     ####
####   to smoothen the two-dimensional histogram of offers. First MA is calculated    ####
####  for commissions partitioned by interest values. In second step MA is calculated ####
####            over the resulting column partitioned by commission values            ####
##########################################################################################

#from time import *
#optStart = clock()

# Initial parameters
abstol    = 0.005    # Precision of price optimization
rnd       = 3        # Number of significant digits
#bandwidth = 50      # How many rows in forward and backward to take for moving average

# Create net of all possible interest rates, rounded to desired abstol
# value 'key0' is a dummy variable to make cartesian product
NIR = hc.range(int(minNIR/abstol), int((maxNIR + abstol)/abstol), 1, 1)
NIR = NIR.withColumn("Interest", psf.round(NIR.id*abstol, rnd)).withColumn("key", psf.lit("key0")).select('key', 'Interest')

# Create net of all possible commission percentages, rounded to desired abstol
# value 'key0' is a dummy variable to make cartesian product
Com = hc.range(int(minCom/abstol), int((maxCom + abstol)/abstol), 1, 1)
Com = Com.withColumn("CommissionPct", psf.round(Com.id*abstol, rnd)).withColumn("key", psf.lit("key0")).select('key', 'CommissionPct')

########################################################################
### New KDE
kdNIR = KernelDensity()
kdCom = KernelDensity()
sampleNIR = MyTrain.select('Interest').rdd.map(lambda x: x[0])
sampleCom = MyTrain.select('CommissionPct').rdd.map(lambda x: x[0])
kdNIR.setSample(sampleNIR)
kdCom.setSample(sampleCom)

NIRs = NIR.select('Interest').rdd.map(lambda x: x[0]).collect()
NIREst = kdNIR.estimate(NIRs).tolist()
kdeNIR = hc.createDataFrame(sc.parallelize(zip(NIRs, NIREst)), ['Interest', 'kdeNIR']).coalesce(1).cache()
Coms = Com.select('CommissionPct').rdd.map(lambda x: x[0]).collect()
ComEst = kdCom.estimate(Coms).tolist()
kdeCom = hc.createDataFrame(sc.parallelize(zip(Coms, ComEst)), ['CommissionPct', 'kdeCom']).coalesce(1).cache()

NIR = NIR.join(kdeNIR, on = 'Interest')
Com = Com.join(kdeCom, on = 'CommissionPct')

# Cartesian product of interest and commission nets - full net of all possible combinations
hist = psf.broadcast(NIR.join(Com, 'key').coalesce(1))
########################################################################
### Old KDE
## Cartesian product of interest and commission nets - full net of all possible combinations
#hist = psf.broadcast(NIR.join(Com, 'key').coalesce(defNumPart))

## Select real data and round to desired abstol
#kdData = MyTrain.select(psf.round(psf.round(MyTrain.Interest/abstol)*abstol, rnd).alias('Interest'),
#                        psf.round(psf.round(MyTrain.CommissionPct/abstol)*abstol, rnd).alias('CommissionPct')).coalesce(defNumPart)

## Create a histogram by grouping and counting
#kdData = kdData.groupby(kdData.Interest, kdData.CommissionPct).count().coalesce(defNumPart)

## Join with all net. Fill count of not-existing offers with 0
#jCnd = ['Interest', 'CommissionPct']
#kdData = hist.select('Interest', 'CommissionPct').join(kdData, jCnd, 'left').fillna(0).coalesce(defNumPart)

## First smoothing: Smooth commissions per each interest
#window = Window.partitionBy('Interest').orderBy('Interest', 'CommissionPct').rowsBetween(-ComBandwidth,ComBandwidth)
#kdSmooth = psf.avg(kdData['count']).over(window)
#kdData = kdData.select('*', kdSmooth.alias('hist')).coalesce(defNumPart)

## Second smoothing: Smooth the resulting column per each commission
#window2 = Window.partitionBy('CommissionPct').orderBy('CommissionPct', 'Interest').rowsBetween(-NIRBandwidth,NIRBandwidth)
#kdSmooth2 = psf.avg(kdData['hist']).over(window2)
#kdData = kdData.select('*', kdSmooth2.alias('kde')).coalesce(defNumPart)
########################################################################

#Get Table
OptDFRaw = hc.read.parquet(scPath).filter('CampaignID = 26 OR CampaignID = 42 OR CampaignID = 43').coalesce(defNumPart).persist()

OptDF = OptDFRaw.fillna(fillNADict)
for f in categoricalVars:
    OptDF = OptDF.replace(woeDict[f][0], woeDict[f][1], f).withColumnRenamed(f, 'woe' + f)
    OptDF = OptDF.withColumn('woe' + f, OptDF['woe' + f].cast('float'))
OptDF = OptDF.fillna(fillWoENADict)

# Create net
optIdVars = ['ClientID', 'CampaignId', 'CampStartDate', 'CreditApplicationID', 'OfferID']
optimVars = list(optIdVars + uncorrelatedVars[2:])
for idx, item in enumerate(optimVars):
    if 'Interest' == item:
        optimVars[idx] = "OrgInterest"
    if 'CommissionPct' == item:
        optimVars[idx] = "OrgCommissionPct"

orgUnCorrVars = list(uncorrelatedVars[2:])
for idx, item in enumerate(orgUnCorrVars):
    if 'Interest' == item:
        orgUnCorrVars[idx] = "OrgInterest"
    if 'CommissionPct' == item:
        orgUnCorrVars[idx] = "OrgCommissionPct"

dataToOptimBasic = OptDF.withColumnRenamed('Interest', 'OrgInterest').withColumnRenamed('CommissionPct', 'OrgCommissionPct').select(optimVars).coalesce(defNumPart)
dataToOptim = dataToOptimBasic.withColumn("key", psf.lit("key0")).coalesce(defNumPart)
dataToOptim = dataToOptim.join(hist, 'key').coalesce(defNumPart)

featureAssembler = VectorAssembler(inputCols = uncorrelatedVars[2:], outputCol = "features")
########################################################################
### New KDE
dataToOptim = featureAssembler.transform(dataToOptim).select(optIdVars + ["features", "Interest", "CommissionPct", 'kdeNIR', 'kdeCom', "NumberofInstallments"]).coalesce(defNumPart)
########################################################################
### Old KDE
#dataToOptim = featureAssembler.transform(dataToOptim).select(optIdVars + ["features", "Interest", "CommissionPct", "NumberofInstallments"]).coalesce(defNumPart)
########################################################################

orgFeatureAssembler = VectorAssembler(inputCols = orgUnCorrVars, outputCol = "features")
orgDataToScore = orgFeatureAssembler.transform(dataToOptimBasic).select(optIdVars + ["features", "OrgInterest", "OrgCommissionPct", "NumberofInstallments"]).coalesce(defNumPart)

getProb = psf.udf(lambda vec: float(vec[1]), DoubleType())

# Compute probabilities
toJoin = ['ClientID', 'CampaignId', 'CampStartDate', 'CreditApplicationID']
orgPrices = model.transform(orgDataToScore).coalesce(defNumPart)
orgPrices = orgPrices.withColumn("orgProb", getProb(orgPrices.probability)).withColumn("orgMargin", orgPrices.OrgInterest + orgPrices.OrgCommissionPct/0.6/orgPrices.NumberofInstallments).select(toJoin + ['orgProb', "OrgInterest", "OrgCommissionPct", "orgMargin"]).coalesce(defNumPart)

pOpt = model.transform(dataToOptim).coalesce(defNumPart)
pOpt = pOpt.withColumn("singleProb", getProb(pOpt.probability)).coalesce(defNumPart)

pOpt = pOpt.withColumn("margin", pOpt.Interest + pOpt.CommissionPct/0.6/pOpt.NumberofInstallments*12.0).coalesce(defNumPart)
pOpt = pOpt.filter(pOpt.margin >= minMargin).filter(pOpt.margin <= maxMargin)
########################################################################
### New KDE
pOpt = pOpt.withColumn("profit", pOpt.singleProb * pOpt.margin * pOpt.kdeNIR * pOpt.kdeCom).coalesce(defNumPart)
########################################################################
### Old KDE
#pOpt = pOpt.join(kdData.select('Interest', 'CommissionPct', 'kde'), ['Interest', 'CommissionPct'], 'inner').coalesce(defNumPart)
#pOpt = pOpt.withColumn("profit", pOpt.singleProb * pOpt.margin * pOpt.kde).coalesce(defNumPart)
########################################################################

windowProfit = Window.partitionBy(['ClientID', 'CampaignId', 'CreditApplicationID'])
maxProfitCol = psf.max(pOpt['profit']).over(windowProfit)
pOpt = pOpt.select('*', maxProfitCol.alias('maxProfit')).coalesce(defNumPart)

prices = pOpt.filter('profit = maxProfit').join(orgPrices, on = toJoin, how = 'inner').select(toJoin + ['OfferID', 'NumberofInstallments', 'Interest', 'CommissionPct', 'singleProb', 'kdeNIR', 'kdeCom', 'margin', 'profit', 'orgProb', 'OrgInterest', 'OrgCommissionPct', 'orgMargin']).coalesce(defNumPart)
result = prices.select(psf.lit(minNIR).alias('MinNIR'),
psf.lit(maxNIR).alias('MaxNIR'),
psf.lit(minCom).alias('MinCom'),
psf.lit(maxCom).alias('MaxCom'),
psf.lit(minMargin).alias('MinMargin'),
psf.lit(maxMargin).alias('MaxMargin'),
psf.lit(NIRBandwidth).alias('NIRBandwidth'),
psf.lit(ComBandwidth).alias('ComBandwidth'),
'*')


if saveToHDFS:
    result.write.mode('overwrite').parquet(outPath)
else:
    result.show(200, truncate = False)
