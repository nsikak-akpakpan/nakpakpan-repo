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

##3.8 GB
#CreditHistoryLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/CreditHistory.csv"
##0.37 GB, 1.4 GB 4 MB 60MB
#BranchVisitLogLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/chan/BranchVisitLog.csv"
#InternetBankingLogLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/chan/InternetBankingLog.csv"
#LoggingContractLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/chan/LoggingContract.csv"
#TokenUsageLogLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/chan/TokenUsageLog.csv"
#
#TypeOfActionLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/dict/TypeOfAction.csv"

CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet").persist()
BranchVisitLog   = hq.read.parquet(HadoopLink + "chan/BranchVisitLog_parquet").persist()
InternetBankingLog   = hq.read.parquet(HadoopLink + "chan/InternetBankingLog_parquet").persist()
LoggingContract   = hq.read.parquet(HadoopLink + "chan/LoggingContract_parquet").persist()
TokenUsageLog   = hq.read.parquet(HadoopLink + "chan/TokenUsageLog_parquet").persist()
TypeOfAction   = hq.read.parquet(HadoopLink + "dict/TypeOfAction_parquet").persist()

### Reading in data

#CreditHistoryLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditHistoryLink)
#
#BranchVisitLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(BranchVisitLogLink)
#InternetBankingLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(InternetBankingLogLink)
#LoggingContract = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(LoggingContractLink)
#TokenUsageLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(TokenUsageLogLink)
#TypeOfAction = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(TypeOfActionLink)
#
#CreditHistoryLog = CreditHistoryLog.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
#BranchVisitLog = BranchVisitLog.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
#InternetBankingLog = InternetBankingLog.withColumn("RegistrationDate", psf.regexp_replace("RegistrationDate","/","-").cast("date"))
#TokenUsageLog = TokenUsageLog.withColumn("AccessDate", psf.regexp_replace("AccessDate","/","-").cast("date"))

CreditHistoryLog.registerTempTable("CreditHistory")
TokenUsageLog.registerTempTable("TokenUsageLog")
InternetBankingLog.registerTempTable("InternetBankingLog")
LoggingContract.registerTempTable("LoggingContract")
BranchVisitLog.registerTempTable("BranchVisitLog")
TypeOfAction.registerTempTable("TypeOfAction")

seip = hq.sql("SELECT DISTINCT ClientID,ReportingDate FROM CreditHistory").persist()
seip.registerTempTable("seip")

logins = hq.sql("\
SELECT lts.ClientID,lts.TypeOfActionID,seip.ReportingDate \
,count(*) as NumDaysActions\
,sum(CASE WHEN DATEDIFF(seip.ReportingDate,lts.RegistrationDate) <= 30 THEN 1 ELSE 0 END) as NumDaysActionsRecently \
,datediff(seip.ReportingDate,min(lts.RegistrationDate)) as DaysSince1stAction \
,datediff(seip.ReportingDate,max(lts.RegistrationDate)) as DaysSinceLastAction \
,sum(cast(lts.NumberOfActions as float)) as NumberOfActions \
,sum(cast(lts.NumberOfActions as float) * CASE WHEN DATEDIFF(seip.ReportingDate,lts.RegistrationDate) <= 30 THEN 1 ELSE 0 END )  NumberOfActionsRecently \
FROM seip \
LEFT OUTER JOIN InternetBankingLog lts \
ON seip.ClientID=lts.ClientID AND seip.ReportingDate >= lts.RegistrationDate \
INNER JOIN TypeOfAction slt \
ON lts.TypeOfActionID = slt.TypeOfActionID \
where lts.TypeOfActionID in (95,184,168,172,176,219,54,37,113,117,238,78,96) \
GROUP BY lts.ClientID, seip.ReportingDate, lts.TypeOfActionID").persist()
logins.registerTempTable("logins")


tokenusage = hq.sql("\
SELECT seip.ClientID, seip.ReportingDate\
,SUM(case when tu.SecurityTypeID = 1 then 1 else 0 end) AS PhisicalTokenUsage\
, SUM(case when tu.SecurityTypeID = 2 then 1 else 0 end) AS JavaTokenUsage \
FROM seip \
LEFT OUTER JOIN TokenUsageLog tu \
ON tu.ClientID = seip.ClientID \
AND tu.AccessDate BETWEEN DATE_ADD(DATE_ADD(seip.ReportingDate,1),-30) \
and seip.ReportingDate \
GROUP BY seip.ClientID,seip.ReportingDate").persist()
tokenusage.registerTempTable("tokenusage")

mv = hq.sql("\
SELECT	seip.ClientID, seip.ReportingDate\
,SUM(CASE WHEN bl.ReportingDate IS NOT NULL THEN 1 ELSE 0 END) AS CurrentMonthVisits\
,SUM(CASE WHEN bl.ReportingDate IS NOT NULL THEN IsCashOperation ELSE 0 END) AS CurrentMonthVisitsCO\
,SUM(CASE WHEN bl.ReportingDate IS NOT NULL THEN IsSalesOperation ELSE 0 END) AS CurrentMonthVisitsSO \
FROM seip \
LEFT OUTER JOIN BranchVisitLog bl \
ON bl.ClientID = seip.ClientID \
AND bl.ReportingDate BETWEEN add_months(DATE_ADD(seip.ReportingDate,1),-1) \
AND seip.ReportingDate \
GROUP BY seip.ClientID,seip.ReportingDate").persist()
mv.registerTempTable("mv")

tr1 = hq.sql("\
SELECT tr.* from seip \
left join logins tr	\
on seip.ClientID = tr.ClientID \
and seip.ReportingDate = tr.ReportingDate and tr.TypeOfActionID in (184,168,172,176)").persist()
tr1.registerTempTable("tr1")

tr = hq.sql("\
select \
    tr1.ClientID,tr1.ReportingDate,min(tr1.DaysSinceLastAction) as DaysSinceLastTransfer \
    ,sum(tr1.NumberOfActionsRecently) / CASE WHEN min(tr1.DaysSince1stAction) <= 30 \
    THEN min(tr1.DaysSince1stAction) + 1 ELSE 30 END as AvgNumTransfersRecently \
from tr1 \
group by tr1.ClientID,tr1.ReportingDate").persist()
tr.registerTempTable("tr")

ChurnChannelVariables = hq.sql("\
select seip.ClientID, seip.ReportingDate, \
COALESCE(l.NumberOfActionsRecently / CASE WHEN l.DaysSince1stAction <= 30 THEN l.DaysSince1stAction + 1 ElSE 30 + 1 END,0) as AvgNumLoginsRecently, \
COALESCE(CASE WHEN l.NumDaysActionsRecently = 0 THEN 0 ELSE l.NumberOfActionsRecently / l.NumDaysActionsRecently END, 0) as AvgNumLoginsOnLoginDaysRecently,\
l.NumberOfActionsRecently / CASE WHEN l.DaysSince1stAction <= 30 THEN l.DaysSince1stAction + 1 ElSE 30 + 1 END - l.NumberOfActions / (l.DaysSince1stAction + 1) as LoginActivityDiff,	\
l.DaysSinceLastAction as DaysSinceLastLogin,\
tr.DaysSinceLastTransfer, COALESCE(tr.AvgNumTransfersRecently, 0) AS AvgNumTransfersRecently, \
COALESCE(dolad.NumDaysActionsRecently, 0) as NumDaysDoladRecently, \
COALESCE(CASE WHEN depo.NumDaysActionsRecently > 0 THEN 1 ELSE 0 END, 0) AS HasOpenedDepositRecently, \
COALESCE(CASE WHEN depoCancel.NumDaysActionsRecently > 0 THEN 1 ELSE 0 END, 0) AS HasClosedDepositRecently, \
COALESCE(CASE WHEN cc.NumDaysActionsRecently > 0 THEN 1 ELSE 0 END, 0) AS HasPayedCCRecently, \
COALESCE(CASE WHEN dc.NumDaysActionsRecently > 0 THEN 1 ELSE 0 END, 0) AS HasActivatedDebitCardRecently, \
COALESCE(CASE WHEN loginsLimit.NumDaysActionsRecently > 0 THEN 1 ELSE 0 END, 0) AS HasReachedLoginsLimitRecently, \
COALESCE(CASE WHEN orders.NumDaysActionsRecently > 0 THEN 1 ELSE 0 END, 0) AS HasNewOrderRecently, \
mv.CurrentMonthVisits, \
LAG(mv.CurrentMonthVisits) OVER (PARTITION BY mv.ClientID ORDER BY mv.ReportingDate) AS LastMonthVisits, \
AVG(CAST(mv.CurrentMonthVisits AS float)) OVER (PARTITION BY mv.ClientID ORDER BY mv.ReportingDate ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS YearlyVisitAvgPerMonth, \
mv.CurrentMonthVisitsCO, \
LAG(mv.CurrentMonthVisitsCO) OVER (PARTITION BY mv.ClientID ORDER BY mv.ReportingDate) AS LastMonthVisitsCO, \
AVG(CAST(mv.CurrentMonthVisits AS float)) OVER (PARTITION BY mv.ClientID ORDER BY mv.ReportingDate ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS YearlyVisitCOAvgPerMonth, \
mv.CurrentMonthVisitsSO, \
LAG(mv.CurrentMonthVisitsSO) OVER (PARTITION BY mv.ClientID ORDER BY mv.ReportingDate) AS LastMonthVisitsSO, \
AVG(CAST(mv.CurrentMonthVisitsSO AS float)) OVER (PARTITION BY mv.ClientID ORDER BY mv.ReportingDate ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS YearlyVisitSOAvgPerMonth, \
tu.PhisicalTokenUsage, tu.JavaTokenUsage, \
CASE WHEN c.ClientID IS NOT NULL then 1 ELSE 0 END AS HasActiveBE \
,cast(months_between(seip.ReportingDate,c.FirstLoginDate) as int) AS MonthsSinceFirstBELogin \
FROM seip \
LEFT OUTER JOIN logins l \
ON seip.ClientID = l.ClientID and seip.ReportingDate = l.ReportingDate AND l.TypeOfActionID = 95	\
left join tr on seip.ClientID=tr.ClientID and seip.ReportingDate=tr.ReportingDate \
left join logins dolad on seip.ClientID = dolad.ClientID and seip.ReportingDate = dolad.ReportingDate and dolad.TypeOfActionID = 219 \
LEFT JOIN logins depo ON seip.ClientID = depo.ClientID AND seip.ReportingDate = depo.ReportingDate AND depo.TypeOfActionID = 113 \
LEFT JOIN logins depoCancel ON l.ClientID = depoCancel.ClientID AND l.ReportingDate= depoCancel.ReportingDate AND depoCancel.TypeOfActionID = 117 \
LEFT JOIN logins chat	ON seip.ClientID = chat.ClientID AND seip.ReportingDate = chat.ReportingDate AND chat.TypeOfActionID = 78 \
LEFT JOIN logins cc ON seip.ClientID = cc.ClientID AND seip.ReportingDate = cc.ReportingDate AND cc.TypeOfActionID = 37 \
LEFT JOIN logins dc ON seip.ClientID = dc.ClientID AND seip.ReportingDate = dc.ReportingDate AND dc.TypeOfActionID = 54 \
LEFT JOIN logins loginsLimit ON seip.ClientID = loginsLimit.ClientID AND seip.ReportingDate= loginsLimit.ReportingDate AND loginsLimit.TypeOfActionID = 96 \
LEFT JOIN logins orders ON seip.ClientID = orders.ClientID AND seip.ReportingDate = orders.ReportingDate AND orders.TypeOfActionID = 238 \
LEFT JOIN	mv ON mv.ClientID = seip.ClientID AND mv.ReportingDate = seip.ReportingDate \
left outer JOIN tokenusage tu ON tu.ClientID = seip.ClientID AND tu.ReportingDate = seip.ReportingDate \
LEFT OUTER JOIN LoggingContract c ON seip.ClientID = c.ClientID and seip.ReportingDate >= c.StartDate AND seip.ReportingDate <= c.EndDate")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnChannelVariables"
#ChurnClientVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnChannelVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnChannelVariables")
