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

#CreditHistoryLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/CreditHistory.csv"
#ComplaintLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/chan/ComplaintLog.csv"

#CreditHistoryLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditHistoryLink)
#ComplaintLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(ComplaintLink)

#CreditHistoryLog2 = CreditHistoryLog.select("ClientID","ContractID",CreditHistoryLog.ReportingDate.substr(1,10).alias("ReportingDate"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))

#ComplaintLog2 = ComplaintLog.select("ClientID",ComplaintLog.ResponseDate.substr(1,10).alias("ResponseDate"),"Complain","ComplaintResultID","ComplaintReasonID")
#ComplaintLog2 = ComplaintLog2.withColumn("ResponseDate", psf.regexp_replace("ResponseDate","/","-").cast("date"))

CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet")
ComplaintLog   = hq.read.parquet(HadoopLink + "chan/ComplaintLog_parquet")

CreditHistoryLog.registerTempTable("CreditHistory")
ComplaintLog.registerTempTable("ComplaintLog")

exposures = hq.sql("\
SELECT \
    DISTINCT ClientID,ReportingDate,add_months(ReportingDate,-3) AS ReportingDateWindowStart \
FROM CreditHistory")
exposures.registerTempTable("exposures")

ChurnComplaintsVariables = hq.sql("\
SELECT \
    e.ClientID,e.ReportingDate \
    ,COUNT(DISTINCT CASE WHEN c.ComplaintReasonID = 5 THEN c.Complain ELSE NULL END) AS NumberOfQueryThreads \
    ,COUNT(DISTINCT CASE WHEN c.ComplaintReasonID = 8 THEN c.Complain ELSE NULL END) AS NumberOfComplaintsThreads \
    ,COUNT(DISTINCT CASE WHEN c.ComplaintReasonID NOT IN  (8,5) THEN c.Complain ELSE NULL END) AS NumberOfOtherThreads \
    ,COUNT(DISTINCT CASE WHEN c.ComplaintResultID = 4 THEN c.Complain else NULL END) AS NumberOfPendingThreads \
    ,COUNT(DISTINCT CASE WHEN c.ComplaintResultID in (9,8) THEN c.Complain else NULL END) AS NumberOfRejectedThreads \
    ,COUNT(DISTINCT CASE WHEN c.ComplaintResultID in (7,3) THEN c.Complain else NULL END) AS NumberOFAcceptedThreads \
FROM exposures e \
LEFT OUTER JOIN ComplaintLog c \
ON e.ClientID = c.ClientID AND c.ResponseDate >= e.ReportingDateWindowStart AND c.ResponseDate <= e.ReportingDate \
GROUP BY e.ClientID,e.ReportingDate")
#ChurnComplaintsVariables.show()

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnComplaintsVariables"
#ChurnComplaintsVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnComplaintsVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnComplaintsVariables")
