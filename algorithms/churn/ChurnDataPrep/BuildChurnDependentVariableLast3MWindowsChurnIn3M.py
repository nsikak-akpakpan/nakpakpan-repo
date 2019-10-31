import sys
from pyspark import SparkContext, SparkConf, StorageLevel, RDD
from pyspark.sql import SQLContext, Row, HiveContext
from pyspark.sql.window import Window
from pyspark.sql import functions as psf

HadoopLink = sys.argv[1]
HadoopLink2 = sys.argv[2]

#conf = SparkConf()
#conf.setMaster("local[*]")
#conf.setAppName("BuildChurnChannelVariables")
#conf.set("spark.executor.memory", "4g")
#conf.set("spark.executor.cores", 2)
#conf.set("spark.jars.packages", "com.databricks:spark-csv_2.11:1.4.0")

sc = SparkContext()
sq = SQLContext(sc)
hq = HiveContext(sc)

###HadoopLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/"

CashLoanForRandomSample   = hq.read.parquet(HadoopLink + "var/CashLoanForRandomSample_parquet").persist()
CashLoanForRandomSample.registerTempTable("CashLoanForRandomSample")

ClientContractDateMapping = hq.read.parquet(HadoopLink + "dict/ClientContractDateMapping_parquet").persist()
ClientContractDateMapping.registerTempTable("ClientContractDateMapping")

SaleOfCredits = hq.read.parquet(HadoopLink + "contr/SaleOfCredits_parquet").persist()
SaleOfCredits.registerTempTable("SaleOfCredits")

max_date = str(CashLoanForRandomSample.agg(psf.max("ReportingDate")).take(1)[0][0])
tag = str(sys.argv[1]) if len(sys.argv) > 1 else 'Last3MWindowsChurnIn3M'
#tag='Last3MWindowsChurnIn3M'


soc = hq.sql("\
SELECT \
    clfrs.ContractID,clfrs.ReportingDate \
    ,COUNT(case when soc.ContractID is not null then 1 else null end) AS ExistFlag \
FROM CashLoanForRandomSample clfrs \
LEFT JOIN SaleOfCredits soc \
ON soc.ContractID = clfrs.ContractID AND soc.SalesDate < clfrs.ReportingDate \
GROUP BY clfrs.ContractID,clfrs.ReportingDate \
")
soc.registerTempTable("soc")

step1 = hq.sql("\
SELECT \
	clfrs.* \
	,COALESCE(clfrs.IsChurnIn3M, clfrs.IsChurnIn2M, clfrs.IsChurnIn1M) as IsChurnWithin3M \
FROM CashLoanForRandomSample clfrs \
LEFT JOIN soc on clfrs.ContractID=soc.ContractID AND clfrs.ReportingDate=soc.ReportingDate \
WHERE \
    clfrs.StartYear >= 2012 \
	AND clfrs.HistoricDPDLessThan30 = 1 \
	AND soc.ExistFlag = 0 \
	AND ABS(clfrs.Balance) > 30 \
	AND months_between(clfrs.EndDate,clfrs.ReportingDate) >= 0 \
	AND ((clfrs.Tenor > 24 AND DATEDIFF(clfrs.PlannedEndDate,clfrs.ReportingDate) >= 90) OR (clfrs.Tenor <= 24 AND DATEDIFF(clfrs.PlannedEndDate,clfrs.ReportingDate) >= 60)) \
	AND (COALESCE(clfrs.IsChurnIn3M, clfrs.IsChurnIn2M, clfrs.IsChurnIn1M) = 1 OR clfrs.IsChurnIn3M = 0) \
	AND clfrs.ReportingDate >= '2011-06-30' \
	AND clfrs.HasNext6MAvailable = 1 \
	AND clfrs.OnBlacklistInLast3Months = 0\
")
step1.registerTempTable("step1")

step2 = hq.sql("SELECT c.ContractID,c.ClientID,c.ReportingDate,c.IsChurnWithin3M as IsChurnIn3M	FROM step1 c")
step2.registerTempTable("step2")

step3 = hq.sql("\
SELECT \
	s.ContractID \
	,s.ClientID \
	,s.ReportingDate \
	,crndm.ChurnID \
	,s.IsChurnIn3M as Target \
FROM step2 s \
LEFT JOIN ClientContractDateMapping crndm \
ON s.ContractID=crndm.ContractID and s.ReportingDate=crndm.ReportingDate and s.ClientID=crndm.ClientID \
WHERE s.ReportingDate='2014-07-31' OR s.ReportingDate='2014-04-30' OR s.ReportingDate='2014-01-31'")
step3.registerTempTable("step3")

ChurnDependentVariable = hq.sql("\
SELECT \
	'"+tag+"' AS Tag \
	,current_date() AS SampleCreationDate \
	,ChurnID \
	,Target \
FROM step3\
")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnDependentVariable"
ChurnDependentVariable.write.mode('overwrite').parquet(HadoopLink2 + "ChurnDependentVariable")
