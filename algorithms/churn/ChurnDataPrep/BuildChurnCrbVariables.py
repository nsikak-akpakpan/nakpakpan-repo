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
CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet")
#CreditHistoryLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditHistoryLink)
#CreditHistoryLog2 = CreditHistoryLog.select("ClientID",CreditHistoryLog.ReportingDate.substr(1,10).alias("ReportingDate"),"Balance",CreditHistoryLog.StartDate.substr(1,10).alias("StartDate"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("StartDate", psf.regexp_replace("StartDate","/","-").cast("date"))
CreditHistoryLog.registerTempTable("CreditHistory")

#CreditBureauRaportLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/crb/CreditBureauRaport.csv"
CreditBureauRaport = hq.read.parquet(HadoopLink + "crb/CreditBureauRaport_parquet")
#CreditBureauRaport = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(CreditBureauRaportLink)
#CreditBureauRaport2 = CreditBureauRaport.withColumn("CreationDate", psf.regexp_replace("CreationDate","/","-").cast("date"))
CreditBureauRaport.registerTempTable("CreditBureauRaport")

#CreditBureauRequestsLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/crb/CreditBureauRequests.csv"
CreditBureauRequests = hq.read.parquet(HadoopLink + "crb/CreditBureauRequests_parquet")
#CreditBureauRequests = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(CreditBureauRequestsLink)
#CreditBureauRequests2 = CreditBureauRequests.withColumn("BIOZDate", psf.regexp_replace("BIOZDate","/","-").cast("date"))
#CreditBureauRequests2 = CreditBureauRequests.withColumn("CreationDate", psf.regexp_replace("CreationDate","/","-").cast("date"))
CreditBureauRequests.registerTempTable("CreditBureauRequests")

#CreditBureauLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/crb/CreditBureau.csv"
CreditBureau = hq.read.parquet(HadoopLink + "crb/CreditBureau_parquet")
#CreditBureau = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(CreditBureauLink)
#CreditBureau2 = CreditBureau.withColumn("ClosureDate", psf.regexp_replace("ClosureDate","/","-").cast("date"))
#CreditBureau2 = CreditBureau.withColumn("ContractDate", psf.regexp_replace("ContractDate","/","-").cast("date"))
CreditBureau.registerTempTable("CreditBureau")


exposures = hq.sql("\
SELECT DISTINCT\
	ClientID\
	,seip.ReportingDate\
	,MIN(seip.StartDate) AS StartDate\
	,SUM(seip.Balance) as Balance \
FROM \
	CreditHistory seip \
group by \
	seip.ClientID \
	,seip.ReportingDate")
exposures.registerTempTable("exposures")

dane1 = hq.sql("\
select\
    e.*\
   	,r1.RaportID as IDDoBikKredyt\
	,r1.CreationDate as DateBikRaport\
	,row_number() over (partition by e.ClientID,e.ReportingDate order by r1.CreationDate desc) as Rnk \
from exposures e \
left join CreditBureauRaport r1 \
on e.ClientID=r1.ClientID and r1.CreationDate < e.ReportingDate \
")
dane1.registerTempTable("dane1")

LatestBikRaport = hq.sql("\
select \
    ClientID,ReportingDate,StartDate,Balance,IDDoBikKredyt,DateBikRaport \
from dane1 \
where Rnk=1\
")
LatestBikRaport.registerTempTable("LatestBikRaport")

queries = hq.sql("\
SELECT \
z.RaportID, z.BIOZDate \
FROM CreditBureauRequests z \
WHERE z.ReasonID = 1 \
")
queries.registerTempTable("queries")

queriesTest = hq.sql("\
select a.IDDoBikKredyt,a.ReportingDate \
from LatestBikRaport a \
left outer join queries b \
on a.IDDoBikKredyt=b.RaportID and months_between(a.ReportingDate,b.BIOZDate) < 4 \
")

queries3M = hq.sql("\
select a.IDDoBikKredyt,a.ReportingDate \
,count(case when b.RaportID is not null then 1 else null end) as QueriesCount3M \
from LatestBikRaport a \
left join queries b \
on a.IDDoBikKredyt=b.RaportID and cast(months_between(a.ReportingDate,b.BIOZDate) as int) <= 3 \
group by a.IDDoBikKredyt,a.ReportingDate \
")
queries3M.registerTempTable("queries3M")

queries1M = hq.sql("\
select a.IDDoBikKredyt,a.ReportingDate \
,count(case when b.RaportID is not null then 1 else null end) as QueriesCount1M \
from LatestBikRaport a \
left join queries b \
on a.IDDoBikKredyt=b.RaportID and cast(months_between(a.ReportingDate,b.BIOZDate) as int) <= 1 \
group by a.IDDoBikKredyt,a.ReportingDate \
")
queries1M.registerTempTable("queries1M")

queriesAtApplication = hq.sql("\
select a.IDDoBikKredyt,a.ReportingDate \
,count(case when b.RaportID is not null then 1 else null end) as QueriesCount1M \
from LatestBikRaport a \
left join queries b \
on a.IDDoBikKredyt=b.RaportID and cast(ABS(months_between(a.StartDate,b.BIOZDate)) as int) <= 1 \
group by a.IDDoBikKredyt,a.ReportingDate\
")
queriesAtApplication.registerTempTable("queriesAtApplication")

ChurnCrbVariables = hq.sql("\
SELECT \
	lbr.ClientID \
	,lbr.ReportingDate \
	,months_between(lbr.ReportingDate,lbr.DateBikRaport) as MonthsSinceLastBikCredit \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.CreditTypeID=18 AND ct.IsOwnCredit = 0 THEN 1 ELSE 0 END) AS ActiveMortgageInOtherBanksCount \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.CreditTypeID in (2,3,4,5) AND ct.IsOwnCredit = 0 THEN 1 ELSE 0 END) AS ActiveCashLoanInOtherBanksCount \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.IsOwnCredit = 0 THEN 1 ELSE 0 END) AS ActiveCreditInOtherBankCount \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.IsOwnCredit = 0 and ct.DPD < 90 THEN 1 ELSE 0 END) AS ActiveCreditNotDefaultedInOtherBankCount \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.IsOwnCredit = 0  THEN ct.Balance ELSE 0 END) AS SumActiveForeignBalance \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.CreditTypeID in (2,3,4,5) THEN 1.0 ELSE 0.0 END) / case when SUM(CASE WHEN ct.CreditTypeID in (2,3,4,5) \
    THEN 1.0 ELSE 0.0 END)=0 then NULL else SUM(CASE WHEN ct.CreditTypeID in (2,3,4,5) THEN 1.0 ELSE 0.0 END) \
    END AS ActiveToAllCashLoansRatio \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate  \
    THEN 1.0 ELSE 0.0 END) / CASE WHEN count(*)=0 THEN NULL ELSE count(*) END AS ActiveToAllCreditsRatio \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.CreditTypeID in (2,3,4,5) AND ct.IsOwnCredit = 1 THEN 1.0 ELSE 0.0 END) / CASE WHEN \
    SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.CreditTypeID in (2,3,4,5) THEN 1.0 ELSE 0.0 END)=0 THEN NULL ELSE SUM(CASE WHEN ct.ClosureDate IS NULL \
    AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate AND ct.CreditTypeID in (2,3,4,5) \
    THEN 1.0 ELSE 0.0 END) END AS ActiveEBToAllActiveCashLoansRatio \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.IsOwnCredit = 1 THEN 1.0 ELSE 0.0 END) / CASE WHEN SUM(CASE WHEN ct.ClosureDate IS NULL \
    AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate THEN 1.0	ELSE 0.0 END)=0 THEN \
    NULL ELSE SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    THEN 1.0 ELSE 0.0 END) END AS ActiveEBToAllActiveCreditsRatio \
	,SUM(CASE WHEN months_between(ct.ClosureDate,ct.ContractDate) < ct.InstallmentPeriod - 1 \
    and ct.CreditTypeID in (2,3,4,5)  then 1 ELSE 0 END) ChurnedCreditHistoryCount 	\
	,SUM(0) InstallmentDiffCount \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.IsOwnCredit = 0  THEN ct.Balance	ELSE 0 END) - SUM(lbr.Balance) AS BalanceDifferance1 \
	,SUM(CASE WHEN ct.ClosureDate IS NULL AND add_months(ct.ContractDate,ct.InstallmentPeriod) > lbr.ReportingDate \
    AND ct.IsOwnCredit = 0  THEN ct.Balance	ELSE 0 END) /SUM(CASE WHEN lbr.Balance=0 THEN NULL ELSE lbr.Balance END) \
    AS BalanceDifferance2 \
	,MAX(CASE WHEN months_between(lbr.ReportingDate,lbr.DateBikRaport) > 3 then -666	\
    ELSE queries3M.QueriesCount3M / (4 - months_between(lbr.ReportingDate,lbr.DateBikRaport)) END) as QueriesPerRecentMth \
	,MAX(queries3M.QueriesCount3M) as QueriesInLast3M \
	,MAX(queries1M.QueriesCount1M) as QueriesInLast1M \
	,MAX(queriesAtApplication.QueriesCount1M) as QueriesAtApplication \
FROM \
	LatestBikRaport lbr  \
	LEFT JOIN CreditBureau ct \
		ON lbr.IDDoBikKredyt = ct.RaportID \
	left join queries3M on lbr.IDDoBikKredyt=queries3M.IDDoBikKredyt and lbr.ReportingDate=queries3M.ReportingDate \
	left join queries1M on lbr.IDDoBikKredyt=queries1M.IDDoBikKredyt and lbr.ReportingDate=queries1M.ReportingDate \
	left join queriesAtApplication on lbr.IDDoBikKredyt=queriesAtApplication.IDDoBikKredyt and lbr.ReportingDate=queriesAtApplication.ReportingDate \
where \
	 ct.RelationshipID = 1 \
GROUP BY \
	lbr.ClientID \
	,lbr.ReportingDate \
	,months_between(lbr.ReportingDate,lbr.DateBikRaport)\
")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnCrbVariables"
#ChurnInboundCallVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnCrbVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnCrbVariables")
