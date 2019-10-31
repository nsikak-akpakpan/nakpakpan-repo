import sys
from pyspark import SparkContext, SparkConf, StorageLevel, RDD
from pyspark.sql import SQLContext, Row, HiveContext
from pyspark.sql.window import Window
from pyspark.sql import functions as psf

#conf = SparkConf()
#conf.setMaster("local[*]")
#conf.setAppName("BuildChurnChannelVariables")
#conf.set("spark.executor.memory", "4g")
#conf.set("spark.executor.cores", 2)
#conf.set("spark.jars.packages", "com.databricks:spark-csv_2.11:1.4.0")

HadoopLink = sys.argv[1]
HadoopLink2 = sys.argv[2]

sc = SparkContext()
sq = SQLContext(sc)
hq = HiveContext(sc)

###HadoopLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/"
###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/"

CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet").repartition(20)
#CreditHistoryLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/CreditHistory.csv"
#CreditHistoryLog = sq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditHistoryLink)
#CreditHistoryLog2 = CreditHistoryLog.select("ClientID",CreditHistoryLog.ReportingDate.substr(1,10).alias("ReportingDate"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
CreditHistoryLog.registerTempTable("CreditHistory")

InboundCallLog   = hq.read.parquet(HadoopLink + "chan/InboundCallLog_parquet").repartition(20)
#InboundCallLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/chan/InboundCallLog.csv"
#InboundCallLog = sq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(InboundCallLink)
#InboundCallLog2 = InboundCallLog.withColumn("CallDate", psf.regexp_replace("CallDate","/","-").cast("date"))
InboundCallLog.registerTempTable("InboundCallLog")

InboundCallReason   = hq.read.parquet(HadoopLink + "chan/InboundCallReason_parquet").repartition(20)
#InboundCallReasonLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/chan/InboundCallReason.csv"
#InboundCallReason = sq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(InboundCallReasonLink)
InboundCallReason.registerTempTable("InboundCallReason")


exposures = hq.sql("\
SELECT DISTINCT \
	ClientID,ReportingDate,DATE_ADD(ADD_MONTHS(DATE_ADD(ReportingDate,1),-3),-1) AS ReportingDateWindowStart \
	,DATE_ADD(ADD_MONTHS(DATE_ADD(ReportingDate,1),-1),-1) AS ReportingDatePrev \
FROM CreditHistory")
exposures.registerTempTable("exposures")

UniqueInbound = hq.sql("\
SELECT \
	CallDate,InboundCallLogID,MAX(ClientID) AS ClientID,SUM(WaitTime_sec) AS WaitTime,SUM(TalkTime_sec) AS TalkTime \
	,MAX(IsAnswered) AS IsAnswered,MAX(IsOffered) AS  IsOffered \
 FROM InboundCallLog \
GROUP BY CallDate,InboundCallLogID")
UniqueInbound.registerTempTable("UniqueInbound")

callReasons = hq.sql("\
SELECT \
	CASE \
		WHEN c.CallReasonID = 23 THEN 'PrematurePayment' \
		WHEN c.CallReasonID = 49 THEN 'Windication' \
		WHEN c.CallReasonID = 50 THEN 'Complaints' \
		WHEN c.CallReasonID = 29 OR c.CallReasonID in (40,27) THEN 'Console' \
		WHEN c.CallReasonID = 41 OR c.CallReasonID IN (44,26,30,36,19,38,1,11,21) THEN 'NewProducts' \
		ELSE 'Other' \
	END AS CallReason \
	,ui.* \
FROM InboundCallReason c \
INNER JOIN UniqueInbound ui \
ON c.InboundCallID = ui.InboundCallLogID")
callReasons.registerTempTable("callReasons")

dane = hq.sql("select \
		e.*,cr.CallReason,cr.WaitTime,cr.TalkTime,cr.IsAnswered,cr.IsOffered  \
from exposures e \
left join callReasons cr \
on e.ClientID=cr.ClientID \
where cr.CallDate > e.ReportingDateWindowStart AND cr.CallDate <= e.ReportingDate")
dane.registerTempTable("dane")

cr3M = hq.sql("\
select \
	ClientID,ReportingDate,SUM(CASE CallReason WHEN 'PrematurePayment' THEN 1 ELSE 0 END) AS PrematurePayment3M \
	,SUM(CASE CallReason WHEN 'Windication' THEN 1 ELSE 0 END) AS Windication3M \
	,SUM(CASE CallReason WHEN 'Complaints' THEN 1 ELSE 0 END) AS Complaints3M \
	,SUM(CASE CallReason WHEN 'Console' THEN 1 ELSE 0 END) AS Console3M \
	,SUM(CASE CallReason WHEN 'NewProducts' THEN 1 ELSE 0 END) AS NewProducts3M \
	,CAST(AVG(WaitTime) as INT) AS AVGWaitTime3M,CAST(AVG(TalkTime) AS INT) AS AVGTalkTime3M \
    ,SUM(IsAnswered) AS IsAnswered3M,SUM(IsOffered) AS IsOffered3M \
from dane \
group by ClientID,ReportingDate")
cr3M.registerTempTable("cr3M")

dane2 = hq.sql("\
select \
	e.*,cr.CallReason,cr.WaitTime,cr.TalkTime,cr.IsAnswered,cr.IsOffered \
from exposures e \
left join CallReasons cr \
on cr.ClientID = e.ClientID \
where cr.CallDate > e.ReportingDatePrev AND cr.CallDate <= e.ReportingDate")
dane2.registerTempTable("dane2")

cr1M = hq.sql("\
select \
	ClientID,ReportingDate,SUM(CASE CallReason WHEN 'PrematurePayment' THEN 1 ELSE 0 END) AS PrematurePayment1M \
	,SUM(CASE CallReason WHEN 'Windication' THEN 1 ELSE 0 END) AS Windication1M \
	,SUM(CASE CallReason WHEN 'Complaints' THEN 1 ELSE 0 END) AS Complaints1M \
	,SUM(CASE CallReason WHEN 'Console' THEN 1 ELSE 0 END) AS Console1M \
	,SUM(CASE CallReason WHEN 'NewProducts' THEN 1 ELSE 0 END) AS NewProducts1M \
	,CAST(AVG(WaitTime) AS INT) AS AVGWaitTime1M,CAST(AVG(TalkTime) AS INT) AS AVGTalkTime1M \
    ,SUM(IsAnswered) AS IsAnswered1M,SUM(IsOffered) AS IsOffered1M \
from dane2 \
group by ClientID,ReportingDate")
cr1M.registerTempTable("cr1M")

ChurnInboundCallVariables = hq.sql("\
SELECT \
	e.ClientID,e.ReportingDate,cr3M.PrematurePayment3M,cr3M.Windication3M,cr3M.Complaints3M,cr3M.Console3M,cr3M.NewProducts3M \
	,cr3M.AVGWaitTime3M,cr3M.AVGTalkTime3M,cr3M.IsAnswered3M,cr3M.IsOffered3M,cr1M.PrematurePayment1M,cr1M.Windication1M \
	,cr1M.Complaints1M,cr1M.Console1M,cr1M.NewProducts1M,cr1M.AVGWaitTime1M,cr1M.AVGTalkTime1M,cr1M.IsAnswered1M,cr1M.IsOffered1M \
FROM exposures e \
left join cr3M \
on e.ClientID=cr3M.ClientID and e.ReportingDate=cr3M.ReportingDate \
left join cr1M \
on e.ClientID=cr1M.ClientID and e.ReportingDate=cr1M.ReportingDate")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnInboundCallVariables"
#ChurnInboundCallVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnInboundCallVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnInboundCallVariables")
