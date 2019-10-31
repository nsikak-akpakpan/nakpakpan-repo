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

CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet").persist()
#CreditHistoryLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/CreditHistory.csv"
#CreditHistoryLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditHistoryLink)
#CreditHistoryLog2 = CreditHistoryLog.select("ClientID",CreditHistoryLog.ReportingDate.substr(1,10).alias("ReportingDate"),"Balance",CreditHistoryLog.StartDate.substr(1,10).alias("StartDate"),CreditHistoryLog.EndDate.substr(1,10).alias("EndDate"),CreditHistoryLog.PlannedEndDate.substr(1,10).alias("PlannedEndDate"),"ContractID","InstallmentCashLoan")
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("StartDate", psf.regexp_replace("StartDate","/","-").cast("date"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("EndDate", psf.regexp_replace("EndDate","/","-").cast("date"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("PlannedEndDate", psf.regexp_replace("PlannedEndDate","/","-").cast("date"))
CreditHistoryLog.registerTempTable("CreditHistory")

CreditApplication = hq.read.parquet(HadoopLink + "appl/CreditApplication_parquet").persist()
#CreditApplicationLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/appl/CreditApplication.csv"
#CreditApplication = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditApplicationLink)
#CreditApplication2 = CreditApplication.withColumn("ApplicationDate", psf.regexp_replace("ApplicationDate","/","-").cast("date"))
#CreditApplication2 = CreditApplication.withColumn("BirthDate", psf.regexp_replace("BirthDate","/","-").cast("date"))
CreditApplication.registerTempTable("CreditApplication")

RiskCategories = hq.read.parquet(HadoopLink + "appl/RiskCategories_parquet").persist()
#RiskCategoriesLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/appl/RiskCategories.csv"
#RiskCategories = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(RiskCategoriesLink)
RiskCategories.registerTempTable("RiskCategories")

ClientSegment = hq.read.parquet(HadoopLink + "seg/ClientSegment_parquet").persist()
#ClientSegmentLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/seg/ClientSegment.csv"
#ClientSegment = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(ClientSegmentLink)
ClientSegment.registerTempTable("ClientSegment")

ClientScore = hq.read.parquet(HadoopLink + "score/ClientScore_parquet").persist()
#ClientScoreLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/score/ClientScore.csv"
#ClientScore = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(ClientScoreLink)
ClientScore.registerTempTable("ClientScore")

AggTransactions = hq.read.parquet(HadoopLink + "contr/AggTransactions_parquet").persist()
#AggTransactionsLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/AggTransactions.csv"
#AggTransactions = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(AggTransactionsLink)
#AggTransactions2 = AggTransactions.select("ClientID","Savings",CreditHistoryLog.TransactionMonthEffective.substr(1,10).alias("TransactionMonthEffective"))
#AggTransactions2 = AggTransactions.withColumn("TransactionMonthEffective", psf.regexp_replace("TransactionMonthEffective","/","-").cast("date"))
AggTransactions.registerTempTable("AggTransactions")

SLRejectionReasonAGGR = hq.sql("\
SELECT \
	CASE \
		WHEN la.RejectionReason = 'Bank' THEN 'BankRejected' \
		WHEN la.RejectionReason = 'Klient' THEN 'ClientRejected' \
		WHEN la.RejectionReason = '' THEN 'NotRejected' \
		ELSE 'Other' \
	END AS StatusAggr \
	,la.RejectionReason \
FROM CreditApplication la \
GROUP BY la.RejectionReason\
").persist()
SLRejectionReasonAGGR.registerTempTable("SLRejectionReasonAGGR")

cte = hq.sql("\
SELECT \
la.ClientID,la.ApplicationDate,la.ContractID,sra.StatusAggr\
,ROW_NUMBER() OVER (PARTITION BY la.ClientID ,la.ApplicationDate ORDER BY CASE \
        WHEN la.ContractID is not null THEN 1 \
		WHEN sra.StatusAggr IS NULL OR sra.StatusAggr = 'ClientRejected' THEN 2 \
		WHEN sra.StatusAggr = 'BankRejected' THEN 3 \
		ELSE 4 \
		END) as n, \
	CASE \
		WHEN la.ContractID is not null THEN 'Accepted' \
		WHEN sra.StatusAggr IS NULL OR sra.StatusAggr = 'ClientRejected' THEN 'ClientRejected' \
		WHEN sra.StatusAggr = 'BankRejected' THEN 'BankRejected' \
		ELSE 'Other' \
	END as Status \
	,la.Income,la.EducationID,rc.RiskCat,cast(months_between(current_date(),BirthDate)/12 as int) as Age,la.RejectionReason,la.OwnResources,\
    la.IncomeTypeID,la.MaritalStatusID,cs.SegmentID as KLNSegment,ChannelID,la.ProductID,la.CreditApplicationID \
FROM CreditApplication la \
LEFT JOIN RiskCategories rc \
on la.CreditApplicationID=rc.CreditApplicationID \
left join ClientSegment cs \
on la.ClientID=cs.ClientID and la.ApplicationDate between cs.ValidFrom and cs.ValidTo \
LEFT  JOIN SLRejectionReasonAGGR sra \
ON la.RejectionReason = sra.RejectionReason\
").persist()
cte.registerTempTable("cte")

LoanApplicationAgg = hq.sql("SELECT * FROM cte WHERE n=1").persist()
LoanApplicationAgg.registerTempTable("LoanApplicationAgg")

enddatesnew = hq.sql("select distinct ContractID,ReportingDate,EndDate,PlannedEndDate from CreditHistory").persist()
enddatesnew.registerTempTable("enddatesnew")

StartDate = hq.sql("\
SELECT \
	seip.ContractID,seip.ClientID,MIN(seip.StartDate) AS StartDate,MIN(add_months(seip.StartDate,-1)) AS StartDatePrev \
	,MAX(months_between(edn.PlannedEndDate,seip.StartDate)) AS Tenor,MAX(case when s.Savings is null then 0 ELSE s.Savings END) AS SavingsInitial \
FROM CreditHistory seip \
LEFT JOIN ClientScore da \
ON seip.ContractID = da.ContractID \
LEFT JOIN enddatesnew edn \
ON seip.ReportingDate = edn.ReportingDate AND seip.ContractID = edn.ContractID \
LEFT JOIN AggTransactions s \
ON seip.ClientID = s.ClientID AND ABS(months_between(s.TransactionMonthEffective,seip.StartDate)) < 1 \
WHERE seip.ProductID = 6 \
GROUP BY seip.ContractID,seip.ClientID\
").persist()
StartDate.registerTempTable("StartDate")

LoanInfoFromApplication = hq.sql("\
SELECT  \
	d.ContractID,d.ClientID,StartDate,la.Age AS Age,la.EducationID EducationID,la.Income Income \
	,la.RiskCat as RiskSegment,la.ChannelID,la.IncomeTypeID IncomeTypeID \
    ,la.MaritalStatusID as MartialStatusID,la.KLNSegment as KLNSegment,la.OwnResources \
	,la.ApplicationDate as ApplicationDate,d.Tenor,d.SavingsInitial \
FROM StartDate d \
inner JOIN LoanApplicationAgg la \
ON la.ContractID = d.ContractID and la.ClientID = d.ClientID\
").persist()
LoanInfoFromApplication.registerTempTable("LoanInfoFromApplication")

seip = hq.sql("\
SELECT \
	seip.ClientID,seip.ContractID,seip.ReportingDate,add_months(seip.ReportingDate,-3) AS ReportingDatePrev \
	,MIN(seip.StartDate) AS StartDate,MAX(seip.Balance) AS Balance \
    ,MAX(seip.InstallmentCashLoan) AS InstallmentCL,MAX(s.Savings) AS Savings \
FROM CreditHistory seip \
LEFT JOIN AggTransactions s \
ON seip.ClientID = s.ClientID and s.TransactionMonthEffective = seip.ReportingDate \
WHERE seip.ProductID = 6 \
GROUP BY seip.ClientID,seip.ContractID,seip.ReportingDate,add_months(seip.ReportingDate,-3) \
").persist()
seip.registerTempTable("seip")

dane = hq.sql("\
select \
    seip.ClientID,seip.ReportingDate,seip.ReportingDatePrev,laa.RiskCat,laa.Income,laa.MaritalStatusID \
    ,laa.OwnResources,laa.StatusAggr \
from seip \
left join LoanApplicationAgg laa \
on seip.ClientID=laa.ClientID \
and laa.ApplicationDate BETWEEN seip.ReportingDatePrev and seip.ReportingDate \
AND laa.ApplicationDate > seip.StartDate\
").persist()
dane.registerTempTable("dane")

daneAgg = hq.sql("\
select \
	ClientID,ReportingDate \
	,COUNT(*) AS NumberOfAllLoanApplications \
	,MAX(RiskCat) AS RecentRiskSegment \
	,MAX(Income) RecentIncome \
	,MAX(MaritalStatusID) AS RecentMartialStatusID \
	,MAX(OwnResources) AS RecentOwnResources \
	,SUM(CASE WHEN StatusAggr = 'BankRejected' THEN 1 ELSE 0 END) AS RecentBankRejected \
	,SUM(CASE WHEN StatusAggr = 'ClientRejected' THEN 1 ELSE 0 END) AS RecentClientRejected \
from dane \
group by ClientID,ReportingDate\
").persist()
daneAgg.registerTempTable("daneAgg")

ChurnLoanApplicationVariables = hq.sql("\
SELECT \
	seip.ContractID,seip.ClientID,seip.ReportingDate,NumberOfAllLoanApplications \
    ,CASE WHEN NumberOfAllLoanApplications > 0 THEN 1 ELSE 0 END HasLoanApplication \
	,CAST(CASE WHEN appl.RecentRiskSegment != lifa.RiskSegment then 1 ELSE 0 END as varchar(1)) AS HasRiskSegmentChange \
	,CAST(CASE WHEN appl.RecentMartialStatusID != lifa.MartialStatusID then 1 ELSE 0 END as varchar(1)) AS HasMartialStatusChange \
	,appl.RecentIncome - lifa.Income AS IncomeChanges \
	,CAST(CASE WHEN appl.RecentBankRejected > 0 THEN 1 ELSE 0 END AS VARCHAR(1)) AS HadBankRejectedApplRecently \
	,CAST(CASE WHEN appl.RecentClientRejected > 0 THEN 1 ELSE 0 END AS VARCHAR(1)) AS HadClientRejectedApplRecently \
	,lifa.Age,lifa.EducationID,lifa.Income,lifa.RiskSegment,lifa.IncomeTypeID,lifa.MartialStatusID,lifa.KLNSegment \
	,months_between(seip.ReportingDate,lifa.ApplicationDate) AS MonthsSinceApplication \
	,seip.Savings/CASE WHEN seip.Balance=0 THEN NULL ELSE seip.Balance END AS SavingsToBalanceVer4 \
FROM seip \
left join daneAgg appl \
on seip.ClientID=appl.ClientID and seip.ReportingDate=appl.ReportingDate \
LEFT JOIN LoanInfoFromApplication lifa \
ON seip.ContractID = lifa.ContractID and seip.ClientID=lifa.ClientID\
")

####HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnLoanApplicationVariables"
#ChurnLoanApplicationVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnLoanApplicationVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnLoanApplicationVariables")
