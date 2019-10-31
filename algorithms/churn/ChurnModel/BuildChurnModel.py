from time import *
from numpy import fill_diagonal, ndarray, array, arange
from pyspark import SparkContext, SparkConf, StorageLevel, RDD
from pyspark.sql import HiveContext
from pyspark.ml.feature import VectorSlicer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
from pyspark.sql import functions as psf

programStart = clock()

############ Setup SparkContext ############
#### Use all cores of the local machine ####
############################################
conf = SparkConf()
##conf.setMaster("local[*]")
conf.setAppName("BuildChurnModel")
##conf.set("spark.executor.memory", "2g")
##conf.set("spark.executor.cores", 2)
##sc = SparkContext(conf = conf)

sc = SparkContext()
sq = SQLContext(sc)
hc = HiveContext(sc)

############################### Data import ######################
#### Import table from Hadoop and infer schema from the file. ####
##################################################################
##HadoopLink      = "hdfs://10.82.187.10:8020/data/Churn/"
HadoopLink      = "/hadoop/hdfs/demobank/PROCESSING/analytics_input/CHURN/"

#MyDF = hc.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(HadoopLink + "WOEInputView.csv")
MyDF = hc.read.parquet('HadoopLink + "WOEInput_parquet").persist()
MyDF.registerTempTable("MyDF")
MyDFNoNA = MyDF.fillna(-10000)

#### Divide the initial data into train and test data sets ####
MyTest = MyDFNoNA.sampleBy("Target", {0: 0.3, 1: 0.3}, seed = 0).persist()
MyTrain = MyDFNoNA.subtract(MyTest).persist()

################## Categorical variables handling ###################
####           Transform string variables to integers.           ####
####      Simple indexing with integers from 0 to number of      ####
#### unique categories starting from the most frequent category. ####
#####################################################################
#for f in categoricalVars:
#    stringIndexer = StringIndexer(inputCol = f, outputCol = 'woe' + f)
#    modelSI = stringIndexer.fit(MyDF)
#    MyDF = modelSI.transform(MyDF)
#    #MyTest = modelSI.transform(MyTest)

###################### Variable selection #######################
#### Find correlated variables and remove one from each pair ####
#################################################################

idVars = MyTrain.columns[0:2]
targetVar = MyTrain.columns[2]
modelVars = MyTrain.columns[3:]
#### Manually removing unnecessary columns ####
####      because they are all NULL        ####
modelVars.remove('Age')
modelVars.remove('AgeSq')
modelVars.remove('AgeCb')
modelVars.remove('CurrentLimit')
modelVars.remove('InitialLimit')
modelVars.remove('LimitReached')
modelVars.remove('InitialLimitEqualsCurrent')
modelVars.remove('LimitReachedInLast3Months')
modelVars.remove('LimitChangedInLast3Months')
modelVars.remove('ExcessPaymentAmmountCurrent')
modelVars.remove('NumberOfExcessPayments3M')
modelVars.remove('ExcessPaymentAmmount3M')
modelVars.remove('StartDate')
#############################################################
#### Assemble all feature columns into one vector column ####
####   called "features" and transform to RDD of lists   ####
#############################################################
allToOne = VectorAssembler(inputCols = modelVars, outputCol = "features")
assembledRDD = allToOne.transform(MyTrain.select(modelVars)).select("features").rdd.map(lambda line: line[0]).persist()

#### Calculate correlation matrix and set it to write mode ####
corM = Statistics.corr(assembledRDD)
corM.setflags(write = True)
fill_diagonal(corM, 0.0)

#######################################################
####     Iterate by rows of correlation matrix     ####
####   If there is a value greater than threshold  ####
#### in the current row set the row and the column ####
####            with same index to 0.              ####
#######################################################
nVar = corM.shape[0]
corToMod = array(corM.tolist()) # This is to ensure that a copy is made
uncorrelatedVars = list(modelVars)
threshold = 0.99
for iter in arange(nVar):
    isCor = sum(abs(corToMod[iter]) > threshold) > 0
    if isCor:
        uncorrelatedVars.remove(modelVars[2 + iter])
        corToMod[:, iter] = 0
        corToMod[iter, :] = 0

######################## Logistic regression ########################
#### Train a logistic regression model on pre-selected variables ####
#####################################################################
#### Prepare data for logistic regression ####
glmFA = VectorAssembler(inputCols = uncorrelatedVars, outputCol = "features")
GLMVars = idVars
GLMVars.extend([targetVar, 'features'])
GLMData = glmFA.transform(MyTrain).select(GLMVars).persist()
GLMData = GLMData.withColumn('Target', GLMData.Target.cast("double"))

#### Train the model ####
glm = LogisticRegression(labelCol = "Target")
model = glm.fit(GLMData)

#### Score the sample ####
trainProbs = model.transform(GLMData)

#### Prepare and score test sample ####
GLMTestData = glmFA.transform(MyTest).select(GLMVars).persist()
GLMTestData = GLMTestData.withColumn('Target', GLMTestData.Target.cast("double"))
testProbs = model.transform(GLMTestData)

#### Test the model performance - Gini ###
modelEvaluator = BinaryClassificationEvaluator(rawPredictionCol = "probability", labelCol = "Target")
trainGini = 2*modelEvaluator.evaluate(trainProbs) - 1
testGini = 2*modelEvaluator.evaluate(testProbs) - 1

print('Model has train Gini ' + str(round(trainGini*100, 2)) + ' %.' )
print('Model has test Gini ' + str(round(testGini*100, 2)) + ' %.' )

##LocalLink = "C:/Users/drodak002/Documents/Customer360/HDFS/"

#### Prepare output table ####
scores = trainProbs.unionAll(testProbs)
#### Extracting second component of probability vector ####
#### Casting to string and using regular expressions   ####
#### is the best method found so far.                  ####
scores = scores.withColumn("score", psf.regexp_extract(scores.probability.cast('string'), "(,0)(.*?)\]", 2).cast('double'))
output = scores.select(['ContractID', 'ReportingDate', 'Target', 'score', 'prediction'])

#output.save(HadoopLink + 'output')
output.write.mode('overwrite').parquet(HadoopLink + 'ChurnOutput_parquet')
#output.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink + 'ChurnOutput.csv')

print('Time elapsed: ' + str(round(clock() - programStart, 3)) + ' seconds.')
