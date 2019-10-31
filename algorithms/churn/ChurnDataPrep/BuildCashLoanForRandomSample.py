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

CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet")
CreditHistoryLog.registerTempTable("CreditHistory")

ClientBlackList = hq.read.parquet(HadoopLink + "cli/ClientBlackList_parquet").persist()
ClientBlackList.registerTempTable("ClientBlackList")

max_date = str(CreditHistoryLog.agg(psf.max("ReportingDate")).take(1)[0][0])

TrueLastDate = hq.sql("\
WITH bigPayment as ( \
SELECT \
	ContractID, \
	ReportingDate, \
	CASE \
		WHEN Balance/CASE WHEN LAG(Balance,1,NULL) OVER (PARTITION BY ContractID ORDER BY ReportingDate)=0 THEN NULL ELSE \
        LAG(Balance, 1, NULL) OVER (PARTITION BY ContractID ORDER BY ReportingDate) END <= 0.1 \
        AND Balance < 0 AND ReportingDate < PlannedEndDate THEN 1 \
		ELSE 0 \
 	END AS HadBigPayment \
FROM CreditHistory\
) \
select \
	ContractID,COALESCE(MIN(CASE WHEN HadBigPayment = 1 THEN ReportingDate else NULL END) \
    ,max(CASE WHEN HadBigPayment = 0 THEN ReportingDate else NULL END)) AS TrueEndRaportingDate \
	,MAX(HadBigPayment) AS HadEverBigPayment \
FROM bigPayment \
GROUP BY ContractID\
").persist()
TrueLastDate.registerTempTable("TrueLastDate")

seip = hq.sql("\
SELECT \
	seip.*,CASE \
		WHEN seip.ReportingDate = tld.TrueEndRaportingDate AND tld.HadEverBigPayment = 1 THEN 666 \
		ELSE seip.ContractStatusID \
	END AS ContractStatusIDMod \
FROM CreditHistory seip \
INNER JOIN TrueLastDate tld \
ON seip.ContractID = tld.ContractID AND seip.ReportingDate <= tld.TrueEndRaportingDate\
").persist()
seip.registerTempTable("seip")

enddatesnew = hq.sql("select distinct ContractID,ReportingDate,EndDate,PlannedEndDate from CreditHistory").persist()
enddatesnew.registerTempTable("enddatesnew")

CashLoanForRandomSample = hq.sql("\
WITH ch1 as \
( \
	SELECT seip.ClientID,seip.ReportingDate,count(case when seip1.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN CreditHistory seip1  \
	ON cast(months_between(seip.ReportingDate, seip1.ReportingDate) as int) BETWEEN 1 AND 3 \
	AND seip.ClientID = seip1.ClientID \
	AND seip1.ProductID = 6 \
	and seip.ContractID != seip1.ContractID \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
,ch2 as \
( \
	SELECT seip.ClientID,seip.ReportingDate,count(case when seip1.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN CreditHistory seip1 \
	ON cast(months_between(seip.ReportingDate, seip1.ReportingDate) as int) BETWEEN 1 AND 3 \
	AND seip.ClientID = seip1.ClientID \
	AND seip1.ProductID = 6 \
	and seip.ContractID != seip1.ContractID \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
,ch3 as \
( \
	SELECT seip.ClientID,seip.ReportingDate,count(case when seip1.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN CreditHistory seip1  \
	ON cast(months_between(seip.ReportingDate, seip1.ReportingDate) as int) BETWEEN 1 AND 3  \
	AND seip.ClientID = seip1.ClientID  \
	AND  seip1.ProductID = 6 \
	and seip.ContractID != seip1.ContractID  \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
,ch4 as \
( \
	SELECT seip.ClientID,seip.ReportingDate,count(case when seip1.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN CreditHistory seip1  \
	ON cast(months_between(seip.ReportingDate, seip1.ReportingDate) as int) BETWEEN 1 AND 3 \
	AND seip.ClientID = seip1.ClientID \
	AND  seip1.ProductID = 6 \
	and seip.ContractID != seip1.ContractID \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
,ch5 as \
(\
	select seip.ClientID,seip.ReportingDate,count(case when s.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN CreditHistory s \
	ON s.ClientID = seip.ClientID \
	and cast(months_between(seip.ReportingDate,s.StartDate) as int) between 3 and 6 \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
,ch6 as \
( \
	SELECT seip.ClientID,seip.ReportingDate,count(case when seip1.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN CreditHistory seip1 \
	ON seip.ContractID = seip1.ContractID \
	and seip1.ReportingDate <= seip.ReportingDate \
	and seip1.DPD > 30 \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
,ch7 as \
( \
	SELECT seip.ClientID,seip.ReportingDate,count(case when bl.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN ClientBlackList bl \
	ON bl.ClientID = seip.ClientID \
	AND cast(months_between(seip.ReportingDate, bl.BlackListMonth) as int) between 0 and 3 \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
,ch8 as \
( \
	SELECT seip.ClientID,seip.ReportingDate,count(case when bl.ClientID is not null then 1 else null end) as ExistFlag \
	FROM seip \
	LEFT JOIN ClientBlackList bl \
	ON bl.ClientID = seip.ClientID \
	AND cast(months_between(seip.ReportingDate, bl.BlackListMonth) as int) between -3 and 0 \
	GROUP BY seip.ClientID,seip.ReportingDate \
) \
SELECT \
	seip.ContractID,seip.ContractPaymentLoan,seip.ClientID,seip.ReportingDate \
	,FLOOR(months_between(seip.StartDate,seip.ReportingDate)/3.0)*3.0 AS MOB \
	,seip.Balance \
	,CAST(YEAR(seip.StartDate) AS varchar(4)) +'_'+ CAST( quarter(seip.StartDate) AS varchar(1)) AS StartTime \
	,CAST(YEAR(edn.PlannedEndDate) AS varchar(4)) +'_'+ CAST( quarter(edn.PlannedEndDate) AS varchar(1)) AS PlannedEndTime \
	,CASE \
		WHEN months_between(seip.StartDate, edn.PlannedEndDate) <= 11 THEN FLOOR(months_between(seip.StartDate, edn.PlannedEndDate)/6.0)*6 \
		ELSE FLOOR(months_between(seip.StartDate, edn.PlannedEndDate)/12.0)*12 \
	end AS Tenor \
	,YEAR(seip.StartDate) AS StartYear,seip.StartDate,edn.EndDate,edn.PlannedEndDate \
	,CASE WHEN ContractStatusID = 6 AND ContractPaymentLoan = '' THEN 1 ELSE 0 END AS IsChurnOld \
	,LEAD(CASE \
			WHEN seip.ContractStatusID = 6 AND seip.ContractPaymentLoan is NULL THEN 1 \
			ELSE 0 \
		END ,1,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn1MOld \
	,LEAD(CASE \
			WHEN seip.ContractStatusID = 6 AND seip.ContractPaymentLoan is NULL THEN 1 \
			ELSE 0 \
		END ,2,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn2MOld \
	,LEAD(CASE \
			WHEN seip.ContractStatusID = 6 AND seip.ContractPaymentLoan is NULL THEN 1 \
			ELSE 0 \
		END ,3,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn3MOld \
	,CASE \
		WHEN months_between(seip.ReportingDate,'"+max_date+"') < 6 THEN 0 \
		ELSE 1 \
	 END AS HasNext6MAvailable \
	,CASE \
		WHEN ((ContractStatusIDMod = 6 AND ContractPaymentLoan is NULL) OR seip.ContractStatusIDMod = 666) AND \
		ch1.ExistFlag=0 THEN 1 \
		ELSE 0 \
	END AS IsChurn \
	,LEAD(CASE \
			WHEN ((seip.ContractStatusIDMod = 6 AND seip.ContractPaymentLoan is NULL) OR seip.ContractStatusIDMod = 666) and \
			ch1.ExistFlag=0 then 1 \
			ELSE 0 \
		END ,1,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn1M \
	,LEAD(CASE \
			WHEN ((seip.ContractStatusIDMod = 6 AND seip.ContractPaymentLoan is NULL) OR seip.ContractStatusIDMod = 666)  AND \
			ch2.ExistFlag=0 THEN 1 \
			ELSE 0 \
		END ,2,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn2M \
	,LEAD(CASE \
			WHEN ((seip.ContractStatusIDMod = 6 AND seip.ContractPaymentLoan is NULL) OR seip.ContractStatusIDMod = 666) AND \
			ch3.ExistFlag=0 THEN 1 \
			ELSE 0 \
		END ,3,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn3M  \
	,LEAD(CASE \
			WHEN ((seip.ContractStatusIDMod = 6 AND seip.ContractPaymentLoan is NULL) OR seip.ContractStatusIDMod = 666)  AND \
			ch4.ExistFlag=0 THEN 1 \
			ELSE 0 \
		END ,4,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn4M \
	,seip.DPD \
	,case \
		when ch5.ExistFlag>0 then 1 \
		else 0 \
	end as HasNewcontrIn3_6M \
	,(CASE \
		WHEN ch6.ExistFlag=0 THEN 1 \
		ELSE 0 \
		END) AS HistoricDPDLessThan30 \
	,case \
		when ch7.ExistFlag>0 then 1 \
		else 0 \
	end AS OnBlacklistInNext3Months\
	,case \
		when ch8.ExistFlag>0 then 1 \
		else 0 \
	end AS OnBlacklistInLast3Months \
	,LEAD(CASE  \
			WHEN seip.ContractStatusID = 6 AND seip.ContractPaymentLoan = '' THEN 1 \
			ELSE 0 \
		END ,6,NULL) OVER (PARTITION BY seip.ContractID order by seip.ReportingDate) as IsChurnIn6M \
FROM seip seip \
LEFT OUTER JOIN enddatesnew edn \
ON seip.ReportingDate = edn.ReportingDate and seip.ContractID = edn.ContractID \
LEFT JOIN ch1 ON seip.ClientID=ch1.ClientID and seip.ReportingDate=ch1.ReportingDate \
LEFT JOIN ch2 ON seip.ClientID=ch2.ClientID and seip.ReportingDate=ch2.ReportingDate \
LEFT JOIN ch3 ON seip.ClientID=ch3.ClientID and seip.ReportingDate=ch3.ReportingDate \
LEFT JOIN ch4 ON seip.ClientID=ch4.ClientID and seip.ReportingDate=ch4.ReportingDate \
LEFT JOIN ch5 ON seip.ClientID=ch5.ClientID and seip.ReportingDate=ch5.ReportingDate \
LEFT JOIN ch6 ON seip.ClientID=ch6.ClientID and seip.ReportingDate=ch6.ReportingDate \
LEFT JOIN ch7 ON seip.ClientID=ch7.ClientID and seip.ReportingDate=ch7.ReportingDate \
LEFT JOIN ch8 ON seip.ClientID=ch8.ClientID and seip.ReportingDate=ch8.ReportingDate \
WHERE seip.ProductID = 6\
")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/CashLoanForRandomSample"
CashLoanForRandomSample.write.mode('overwrite').parquet(HadoopLink2 + "CashLoanForRandomSample")
