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
#TransactionsLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/Transactions.csv"

#CreditHistoryLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditHistoryLink)
#TransactionsLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(TransactionsLink)
CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet")
TransactionsLog   = hq.read.parquet(HadoopLink + "contr/Transactions_parquet")

#CreditHistoryLog2 = CreditHistoryLog.select("ClientID","ContractID",CreditHistoryLog.ReportingDate.substr(1,10).alias("ReportingDate"),"InstallmentCashLoan","Balance")
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
CreditHistoryLog.registerTempTable("CreditHistory")
UniqueClientIDRefNumMap = hq.sql("SELECT DISTINCT ContractID, ClientID FROM CreditHistory")
UniqueClientIDRefNumMap.registerTempTable("UniqueClientIDRefNumMap")

transactions = TransactionsLog.filter("TransactionTypeID = 16").select("ClientID","ContractID","Amount",TransactionsLog.TransactionDateEffective.substr(1,10).alias("TransactionDateEffective"))
transactions.registerTempTable("transactions")

ep = hq.sql("\
SELECT DISTINCT \
    ep.*,ucinm.ClientID as UcinmClientID \
FROM transactions ep \
inner join UniqueClientIDRefNumMap ucinm \
ON ep.ContractID = ucinm.ContractID")
#ep = ep.withColumn("TransactionDateEffective", psf.regexp_replace("TransactionDateEffective","/","-").cast("date"))
ep.registerTempTable("ep")

seipC = hq.sql("\
SELECT distinct	\
    ClientID,ContractID,ReportingDate,add_months(ReportingDate,-3) AS ReportingDateFrom\
    ,add_months(ReportingDate,-6) AS ReportingDateFrom6M,InstallmentCashLoan,ABS(Balance) AS Balance \
FROM CreditHistory")
seipC.registerTempTable("seipC")

dane1 = hq.sql("\
select \
    c.ContractID,c.ReportingDate,c.ReportingDateFrom,ep.ContractID as Control,ep.Amount \
from seipC c \
left join ep \
on c.ContractID=ep.ContractID \
where  ep.TransactionDateEffective >= c.ReportingDateFrom and ep.TransactionDateEffective <= c.ReportingDate")
dane1.registerTempTable("dane1")

EP3M = hq.sql("\
select \
    ContractID,ReportingDate,count(case when Control is not null then 1 else null end) AS NumberOfPayments,SUM(Amount) AS PaymentAmmount \
from dane1 \
group by ContractID,ReportingDate")
EP3M.registerTempTable("EP3M")

dane2 = hq.sql("\
select \
    c.ContractID,c.ReportingDate,c.ReportingDateFrom,ep.ContractID as Control,ep.Amount \
from seipC c \
left join ep \
on c.ContractID=ep.ContractID \
where ep.TransactionDateEffective >= c.ReportingDateFrom6M and ep.TransactionDateEffective <= c.ReportingDate")
dane2.registerTempTable("dane2")

EP6M = hq.sql("\
select \
    ContractID,ReportingDate,count(case when Control is not null then 1 else null end) AS NumberOfPayments,SUM(Amount) AS PaymentAmmount \
from dane2 \
group by ContractID,ReportingDate")
EP6M.registerTempTable("EP6M")

dane3 = hq.sql("\
select \
    c.ClientID,c.ReportingDate,c.ReportingDateFrom,ep.ContractID as Control,ep.Amount \
from seipC c \
left join ep \
on c.ClientID=ep.ClientID \
where  ep.TransactionDateEffective <= c.ReportingDate")
dane3.registerTempTable("dane3")

EPHist = hq.sql("\
select \
    ClientID,ReportingDate,count(case when Control is not null then 1 else null end) AS NumberOfPayments,SUM(Amount) AS PaymentAmmount \
from dane3 \
group by ClientID,ReportingDate")
EPHist.registerTempTable("EPHist")

ChurnExcessPayemntVariables = hq.sql("SELECT c.ContractID,c.ReportingDate,c.ClientID\
,case when ISNULL(e.Amount) then 0 else e.Amount end/case when ABS(c.InstallmentCashLoan)=0 then null else ABS(c.InstallmentCashLoan) end as ExcessPaymentAmmountCurrent\
,case when ISNULL(EP3M.NumberOfPayments) then 0 else EP3M.NumberOfPayments end as NumberOfExcessPayments3M\
,case when ISNULL(EP3M.PaymentAmmount) then 0 else EP3M.PaymentAmmount end/case when ABS(c.InstallmentCashLoan)=0 then null else ABS(c.InstallmentCashLoan) end AS ExcessPaymentAmmount3M\
,case when isnull(EP6M.NumberOfPayments) then 0 else EP6M.NumberOfPayments end as NumberOfExcessPayments6M\
,case when isnull(EP6M.PaymentAmmount) then 0 else EP6M.PaymentAmmount end/case when ABS(c.InstallmentCashLoan)=0 then null else ABS(c.InstallmentCashLoan) end AS ExcessPaymentAmmount6M\
,case when isnull(EPHist.NumberOfPayments) then 0 else EPHist.NumberOfPayments end as NumberOfExcessPaymentsHist\
,case when isnull(EPHist.PaymentAmmount) or EPHist.PaymentAmmount=0 then 0 else 1 end AS HasExcessPaymentAmmountHist \
FROM seipC c \
LEFT OUTER JOIN ep e ON c.ContractID = e.ContractID \
and c.ReportingDate = e.TransactionDateEffective	\
left join EP3M on c.ContractID=EP3M.ContractID and c.ReportingDate=EP3M.ReportingDate   \
left join EP6M on c.ContractID=EP6M.ContractID and c.ReportingDate=EP6M.ReportingDate \
left join EPHist on c.ClientID=EPHist.ClientID and c.ReportingDate=EPHist.ReportingDate \
")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnExcessPayemntVariables"
#ChurnExcessPayemntVariables.write.format('com.databricks.spark.csv').mode('overwrite').save(HadoopLink2)
ChurnExcessPayemntVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnExcessPayemntVariables")
