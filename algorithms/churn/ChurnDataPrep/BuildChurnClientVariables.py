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
#conf.set("spark.jars.packages", "com.databricks:spark-csv_2.11:1.4.0")

sc = SparkContext()
sq = SQLContext(sc)
hq = HiveContext(sc)

###HadoopLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/"

CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet")
CreditHistoryLog.registerTempTable("CreditHistory")

Client = hq.read.parquet(HadoopLink + "cli/Client_parquet")
Client.registerTempTable("Client")
ClientBlackList = hq.read.parquet(HadoopLink + "cli/ClientBlackList_parquet")
ClientBlackList.registerTempTable("ClientBlackList")

seip = hq.sql("\
SELECT distinct	ClientID,ReportingDate FROM CreditHistory")
seip.registerTempTable("seip")

ChurnClientVariables = hq.sql("\
with dane as \
( \
    select \
		a.ClientID,a.ReportingDate,count(case when bl1.BlackListMonth is not null then 1 else null end) as HowManyTimesOnBlackList \
	from seip a \
	left join ClientBlackList bl1 ON bl1.ClientID = a.ClientID and bl1.BlackListMonth <= a.ReportingDate \
	group by a.ClientID,a.ReportingDate \
) \
SELECT \
	seip.ClientID,seip.ReportingDate \
    ,ebe.IsBankEmployee,CASE WHEN bl.ClientID IS NULL THEN 0 ELSE 1 END AS IsOnBlackList \
    ,case when dane.HowManyTimesOnBlackList>0 then 1 else 0 end as WasEverOnBlackList \
FROM seip \
LEFT OUTER JOIN	Client dc \
ON seip.ClientID = dc.clientID AND seip.ReportingDate BETWEEN dc.ValidFrom AND dc.ValidTo \
LEFT OUTER JOIN	(select distinct ClientID,IsBankEmployee from Client) ebe \
ON seip.ClientID = ebe.ClientID \
LEFT OUTER JOIN	ClientBlackList bl \
ON seip.ClientID = bl.ClientID AND seip.ReportingDate = bl.BlackListMonth \
LEFT JOIN dane ON seip.clientID=dane.ClientID and seip.ReportingDate=dane.ReportingDate")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnClientVariables"
#ChurnClientVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnClientVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnClientVariables")