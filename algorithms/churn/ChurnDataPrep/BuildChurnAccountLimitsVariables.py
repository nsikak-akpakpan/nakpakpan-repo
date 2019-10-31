from pyspark import SparkContext, SparkConf, StorageLevel, RDD, sys
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

sc = SparkContext()
sq = SQLContext(sc)
hq = HiveContext(sc)

#DepoHistoryLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/DepositHistory.csv"
#AccountPropertiesLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/AccountProperties.csv"
###HadoopLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/"

#DepoHistoryLog = sq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(DepoHistoryLink)
#AccountPropertiesLog = sq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(AccountPropertiesLink)
DepoHistoryLog   = sq.read.parquet(HadoopLink + "contr/DepositHistory_parquet")
AccountPropertiesLog = sq.read.parquet(HadoopLink + "contr/AccountProperties_parquet")

#DepoHistoryLog2 = DepoHistoryLog.select("ContractID","ClientID",DepoHistoryLog.ReportingDate.substr(1,10).alias("ReportingDate"))
#DepoHistoryLog2 = DepoHistoryLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))

#AccountPropertiesLog2 = AccountPropertiesLog.select("ContractID",AccountPropertiesLog.ReportingDate.substr(1,10).alias("ReportingDate"),"CurrentLimit","InitialLimit","LimitReached")
#AccountPropertiesLog2 = AccountPropertiesLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))

DepoHistoryLog.registerTempTable("DepositHistory")
AccountPropertiesLog.registerTempTable("AccountProperties")

ChurnAccountLimitsVariables = sq.sql("\
SELECT \
al.ReportingDate, r.ContractID as RORRefNum, r.ClientID, al.ContractID, \
al.CurrentLimit, al.InitialLimit, al.LimitReached, \
(CASE WHEN al.InitialLimit = al.CurrentLimit THEN 1	ELSE 0 END) AS InitialLimitEqualsCurrent, \
(CASE WHEN SUM(al2.LimitReached)  > 2 THEN 2 + al.LimitReached ELSE SUM(al2.LimitReached) END) AS LimitReachedInLast3Months, \
(CASE WHEN MAX(al2.CurrentLimit) != MIN(al2.CurrentLimit) THEN 1 ELSE 0 END) AS LimitChangedInLast3Months \
FROM AccountProperties al \
LEFT OUTER JOIN DepositHistory r \
ON r.ContractID = al.ContractID AND r.ReportingDate =al.ReportingDate \
JOIN AccountProperties al2 \
ON al.ContractID = al2.ContractID AND al2.ReportingDate BETWEEN add_months(al.ReportingDate,-3) AND al.ReportingDate \
GROUP BY al.ReportingDate, al.ContractID, r.ContractID, r.ClientID, al.CurrentLimit, al.LimitReached, al.InitialLimit\
")#.filter("ContractID=13512290").show()
#ChurnAccountLimitsVariables.show()

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnAccountLimitsVariables"
#ChurnAccountLimitsVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnAccountLimitsVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnAccountLimitsVariables")