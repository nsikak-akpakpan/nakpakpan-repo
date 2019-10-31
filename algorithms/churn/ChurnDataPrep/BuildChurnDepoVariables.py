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
#CreditHistoryLog = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(CreditHistoryLink)
#CreditHistoryLog2 = CreditHistoryLog.select("ClientID",CreditHistoryLog.ReportingDate.substr(1,10).alias("ReportingDate"))
#CreditHistoryLog2 = CreditHistoryLog2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
CreditHistoryLog   = hq.read.parquet(HadoopLink + "contr/CreditHistory_parquet")
CreditHistoryLog.registerTempTable("CreditHistory")

#TransactionsLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/Transactions.csv"
#Transactions = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(TransactionsLink)
#Transactions2 = Transactions.select("ContractID",Transactions.TransactionDateEffective.substr(1,10).alias("TransactionDateEffective"),"TransactionTypeID","Amount","PartyAccountNo","PartyAccountBank","SameSenderName","SameSenderSurname").filter("TransactionTypeID in (17,18)")
#Transactions2 = Transactions2.withColumn("TransactionDateEffective", psf.regexp_replace("TransactionDateEffective","/","-").cast("date"))
Transactions   = hq.read.parquet(HadoopLink + "contr/Transactions_parquet")
Transactions.registerTempTable("Transactions")

#AggTransactionsLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/AggTransactions.csv"
#AggTransactions = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ";", inferschema='true').load(AggTransactionsLink)
#AggTransactions2 = AggTransactions.select("ClientID","ContractID","Savings",AggTransactions.TransactionMonthEffective.substr(1,10).alias("TransactionMonthEffective"),"NumberOfOutgoingStandingOrders","SumOfOutgoingStandingOrders","SumOfOperations","NumberOfOperations","NumberOfCashPaymentsInATM","NumberOfCashPaymentsInBranch","NumTransfersIn","SumOfCashWithdrawalsInBranch","SumOfCashWithdrawalsInATM","AmountTransfersOut","SumOfOutgoingPaymentOrders","SumOfOutgoingStandingOrders","SumOfOutgoingExpressTransfers")
#AggTransactions2 = AggTransactions.withColumn("TransactionMonthEffective", psf.regexp_replace("TransactionMonthEffective","/","-").cast("date"))
AggTransactions   = hq.read.parquet(HadoopLink + "contr/AggTransactions_parquet")
AggTransactions.registerTempTable("AggTransactions")

#DepositHistoryLink = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/contr/DepositHistory.csv"
#DepositHistory = hq.read.format('com.databricks.spark.csv').options(header='true', delimiter = ",", inferschema='true').load(DepositHistoryLink)
#DepositHistory2 = DepositHistory.select("ClientID","ContractID",DepositHistory.ReportingDate.substr(1,10).alias("ReportingDate"),DepositHistory.StartDate.substr(1,10).alias("StartDate"),"Balance","bankActivityFlag","SalaryDepositFlag")
#DepositHistory2 = DepositHistory2.withColumn("ReportingDate", psf.regexp_replace("ReportingDate","/","-").cast("date"))
#DepositHistory2 = DepositHistory2.withColumn("StartDate", psf.regexp_replace("StartDate","/","-").cast("date"))
DepositHistory   = hq.read.parquet(HadoopLink + "contr/DepositHistory_parquet").persist()
DepositHistory.registerTempTable("DepositHistory")

seip = hq.sql("SELECT distinct ClientID, ReportingDate FROM CreditHistory").persist()
seip.registerTempTable("seip")

rorTransfersUnique = hq.sql("\
SELECT \
	rt.ContractID as RefNum,rt.TransactionDateEffective as ReportingDate,rt.Amount,rt.PartyAccountNo as SenderAccNum, \
	rt.PartyAccountBank AS SenderBank,MAX(rt.SameSenderName) AS SameSenderName,MAX(rt.SameSenderSurname) AS SameSenderSurname, \
	COUNT(*) AS cnt \
FROM (select * from Transactions where TransactionTypeID=17) rt \
GROUP BY rt.ContractID,rt.TransactionDateEffective,rt.Amount,rt.PartyAccountNo,rt.PartyAccountBank \
")
rorTransfersUnique.registerTempTable("rorTransfersUnique")

rorTransfersByBank = hq.sql("\
SELECT \
	rtu.RefNum,rtu.ReportingDate,rtu.SenderBank,rtu.SameSenderName * rtu.SameSenderSurname AS SameNameSurname, \
	SUM(rtu.Amount) AS Amount,COUNT(*) AS TransfersCount \
FROM rorTransfersUnique rtu \
GROUP BY rtu.RefNum,rtu.ReportingDate,rtu.SenderBank,rtu.SameSenderName * rtu.SameSenderSurname\
").persist()
rorTransfersByBank.registerTempTable("rorTransfersByBank")

rorOutboundTransfersUnique = hq.sql("\
SELECT \
	rt.ContractID as RefNum,rt.TransactionDateEffective as ReportingDate,rt.Amount,rt.PartyAccountNo as AddresseeAccNum, \
	rt.PartyAccountBank AS AddresseeBank,MAX(rt.SameSenderName) AS SameAddresseeName,MAX(rt.SameSenderSurname) AS SameAddresseeSurname, \
	COUNT(*) AS cnt \
FROM (select * from Transactions where TransactionTypeID=18) rt \
GROUP BY rt.ContractID,rt.TransactionDateEffective,rt.Amount,rt.PartyAccountNo,rt.PartyAccountBank\
")
rorOutboundTransfersUnique.registerTempTable("rorOutboundTransfersUnique")

rorOutboundTransfersByBank = hq.sql("\
SELECT \
	rtu.RefNum,rtu.ReportingDate,rtu.AddresseeBank,rtu.SameAddresseeName * rtu.SameAddresseeSurname AS SameNameSurname, \
	SUM(rtu.Amount) AS Amount,COUNT(*) AS TransfersCount \
FROM rorOutboundTransfersUnique rtu \
GROUP BY rtu.RefNum,rtu.ReportingDate,rtu.AddresseeBank,rtu.SameAddresseeName * rtu.SameAddresseeSurname\
").persist()
rorOutboundTransfersByBank.registerTempTable("rorOutboundTransfersByBank")

oper = hq.sql("\
SELECT \
	ro.ContractID,ro.TransactionMonthEffective as ReportingDate, \
	CASE WHEN ro.NumberOfOutgoingStandingOrders > 0 THEN 1 ELSE 0 END as HasStandingOrder\
	,CASE WHEN ro.NumberOfOutgoingStandingOrders = 0 AND LAG(ro.SumOfOutgoingStandingOrders, 1, 0) OVER \
    (PARTITION BY ro.ContractID ORDER BY ro.TransactionMonthEffective) > 0 THEN 1 ELSE 0 END as  HasCancelledStandingOrders \
	,ro.SumOfOperations,ro.NumberOfOperations \
	,SUM(ro.NumberOfCashPaymentsInATM) OVER (PARTITION BY ro.ContractID ORDER BY ro.TransactionMonthEffective \
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS NumberOfCashPaymentsInATMRecently \
	,SUM(ro.NumberOfCashPaymentsInBranch) OVER (PARTITION BY ro.ContractID ORDER BY ro.TransactionMonthEffective \
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS NumberOfCashPaymentsInBranchRecently \
	,SUM(ro.NumTransfersIn) OVER (PARTITION BY ro.ContractID ORDER BY ro.TransactionMonthEffective \
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS NumberOfIncommingTransfersRecently \
	,AVG(ro.SumOfCashWithdrawalsInBranch + ro.SumOfCashWithdrawalsInATM + ro.AmountTransfersOut + ro.SumOfOutgoingPaymentOrders + ro.SumOfOutgoingStandingOrders + ro.SumOfOutgoingExpressTransfers) \
	OVER (PARTITION BY ro.ContractID ORDER BY ro.TransactionMonthEffective ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW) AS AvgSumOutgoingOperations \
	,MAX(ro.SumOfCashWithdrawalsInBranch + ro.SumOfCashWithdrawalsInATM + ro.AmountTransfersOut + ro.SumOfOutgoingPaymentOrders + ro.SumOfOutgoingStandingOrders + ro.SumOfOutgoingExpressTransfers) \
	OVER (PARTITION BY ro.ContractID ORDER BY ro.TransactionMonthEffective ROWS BETWEEN 2 PRECEDING and CURRENT ROW) AS MaxSumOutgoingOperationsRecently \
	,CASE WHEN MAX(ro.SumOfCashPaymentsInBranch + ro.SumOfCashWithdrawalsInATM) OVER (PARTITION BY ro.ContractID \
    ORDER BY ro.TransactionMonthEffective ROWS BETWEEN 2 PRECEDING and CURRENT ROW) > 0 THEN 1 ELSE 0 END HadBranchOperationRecently \
FROM AggTransactions ro\
").persist()
oper.registerTempTable("oper")

ChurnDepoVariables = hq.sql("\
WITH rorVars AS ( \
SELECT \
	r.ClientID,r.ReportingDate,MAX(months_between(r.ReportingDate,r.StartDate)) AS AccountDurationTime,\
	SUM(CASE WHEN  r.Balance * r.BankActivityFlag > 0 THEN 1 ELSE 0 END) AS NumNonZeroAccounts,\
	sum(r.BankActivityFlag) AS NumAccounts,SUM(r.Balance) AS TotalRORBalance,\
	AVG(sum(r.Balance)) OVER (PARTITION BY r.ClientID ORDER BY r.reportingdate ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS AvgBalancePrevMonths,\
	MAX(r.SalaryDepositFlag) as IsDepositingSalary,SUM(tbb.Amount) AS TotalTransferAmount,SUM(tbb.TransfersCount) AS TotalTransferCount,\
	SUM(tbb.Amount * tbb.SameNameSurname) AS SameNameSurnameAmount,SUM(tbb.TransfersCount * tbb.SameNameSurname) AS SameNameSurnameCount \
	,MAX(op.HasStandingOrder) AS HasStandingOrder,MAX(op.HasCancelledStandingOrders) AS HasCancelledStandingOrders \
	,sum(op.SumOfOperations) AS SumOfOperations,SUM(op.NumberOfOperations) AS NumberOfOperations \
	,CASE \
		WHEN (sum(op.NumberOfCashPaymentsInATMRecently) > 0 OR sum( op.NumberOfCashPaymentsInBranchRecently) > 0) AND sum(op.NumberOfIncommingTransfersRecently) = 0 THEN 'CashOnlyPayment' \
		WHEN sum(op.NumberOfCashPaymentsInATMRecently) = 0 AND sum(op.NumberOfCashPaymentsInBranchRecently) = 0 AND sum(op.NumberOfIncommingTransfersRecently)> 0 THEN 'TransferOnlyPayment' \
		WHEN (sum(op.NumberOfCashPaymentsInATMRecently) > 0 OR  sum(op.NumberOfCashPaymentsInBranchRecently) > 0) AND sum(op.NumberOfIncommingTransfersRecently) > 0 THEN 'MixedPayment' \
		ELSE 'LackOfTransfer' \
	END AS ClientPaymentType \
	,AVG(op.AvgSumOutgoingOperations) AS AvgSumOutgoingOperations,MAX(op.MaxSumOutgoingOperationsRecently) AS MaxSumOutgoingOperationsRecently \
	,CASE \
		WHEN AVG(op.AvgSumOutgoingOperations) = 0 THEN 1 \
		ELSE MAX(op.MaxSumOutgoingOperationsRecently) / AVG(op.AvgSumOutgoingOperations) \
		END AS SumOutgoingOpsRatio \
	,MAX(op.HadBranchOperationRecently) AS HadBranchOperationRecently \
FROM DepositHistory r \
LEFT OUTER JOIN oper op \
ON r.ContractID = op.ContractID AND r.ReportingDate = op.ReportingDate \
LEFT OUTER JOIN rorTransfersByBank tbb \
ON tbb.RefNum = r.ContractID AND tbb.ReportingDate = r.ReportingDate \
GROUP BY r.ClientID,r.ReportingDate\
),senderBanks AS ( \
SELECT \
	r.ClientID,r.ReportingDate,tbb.SenderBank,SUM(tbb.Amount) AS Amount,COUNT(*) AS TransfersCount, \
	ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY SUM(tbb.Amount) DESC) AS BankRnk \
	,ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY COUNT(*) DESC) AS BankRnkCnt \
from DepositHistory r \
LEFT OUTER JOIN rorTransfersByBank tbb \
ON tbb.RefNum = r.ContractID AND months_between(r.ReportingDate,tbb.ReportingDate) >= 1 and months_between(r.ReportingDate,tbb.ReportingDate) < 3 \
GROUP by r.ClientID,r.ReportingDate,tbb.SenderBank\
),senderBanks_zjebane AS ( \
SELECT \
	r.ClientID,r.ReportingDate,tbb.SenderBank,SUM(tbb.Amount) AS Amount,COUNT(*) AS TransfersCount, \
	ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY SUM(tbb.Amount) DESC) AS BankRnk \
	,ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY COUNT(*) DESC) AS BankRnkCnt \
from DepositHistory r \
LEFT OUTER JOIN rorTransfersByBank tbb \
ON tbb.RefNum = r.ContractID AND months_between(r.ReportingDate,tbb.ReportingDate) >= -6 and months_between(r.ReportingDate,tbb.ReportingDate) < 3 \
GROUP by r.ClientID,r.ReportingDate,tbb.SenderBank\
),addresseeBanks AS ( \
SELECT \
	r.ClientID,r.ReportingDate,tbb.AddresseeBank,SUM(tbb.Amount) AS Amount,COUNT(*) AS TransfersCnt, \
	ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY SUM(tbb.Amount) DESC) AS BankRnk, \
	ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY count(*) DESC) AS BankRnkCnt \
from DepositHistory r \
LEFT OUTER JOIN rorOutboundTransfersByBank tbb \
ON tbb.RefNum = r.ContractID AND months_between(r.ReportingDate,tbb.ReportingDate) >= 1 and months_between(r.ReportingDate,tbb.ReportingDate) < 3 \
GROUP by r.ClientID,r.ReportingDate,tbb.AddresseeBank\
),addresseeBanks_zjebane AS( \
SELECT \
	r.ClientID,r.ReportingDate,tbb.AddresseeBank,SUM(tbb.Amount) AS Amount,COUNT(*) AS TransfersCnt, \
	ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY SUM(tbb.Amount) DESC) AS BankRnk, \
	ROW_NUMBER() OVER (PARTITION BY r.ClientID, r.ReportingDate ORDER BY count(*) DESC) AS BankRnkCnt \
from DepositHistory r \
LEFT OUTER JOIN rorOutboundTransfersByBank tbb \
ON tbb.RefNum = r.ContractID AND months_between(r.ReportingDate,tbb.ReportingDate) >= -5 AND months_between(r.ReportingDate,tbb.ReportingDate) < 3 \
GROUP by r.ClientID,r.ReportingDate,tbb.AddresseeBank \
) \
SELECT \
	seip.ClientID,seip.ReportingDate,COALESCE(rV.NumNonZeroAccounts, 0) AS NumNonZeroAccounts,COALESCE(rV.NumAccounts, 0) AS NumAccounts \
	,CASE WHEN COALESCE(rV.NumAccounts - MAX(rV.NumAccounts) OVER (PARTITION BY seip.ClientID ORDER BY seip.ReportingDate \
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 0) > 0 THEN 1 ELSE 0 end AS HasOpenedAccountRecently \
	,CASE WHEN COALESCE(rV.NumAccounts - MAX(rV.NumAccounts) OVER (PARTITION BY seip.ClientID ORDER BY seip.ReportingDate \
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 0) < 0 THEN 1 ELSE 0 end AS HasClosedAccountRecently \
	,COALESCE(rV.TotalRORBalance,0) AS TotalRORBalance,COALESCE(rV.TotalRORBalance,0)-COALESCE(rV.AvgBalancePrevMonths, 0) AS TotalBalanceDiffVsRecently \
	,COALESCE(rV.TotalTransferAmount,0) AS TotalTransferAmount,COALESCE(rV.TotalTransferCount,0) AS TotalTransferCount \
	,COALESCE(rV.SameNameSurnameAmount,0) AS SameNameSurnameAmount,COALESCE(rV.SameNameSurnameCount,0) AS SameNameSurnameCount \
	,sb.SenderBank as MainSenderBank,ab.AddresseeBank as MainAddresseeBank,sbz.SenderBank as MainSenderBank_zjebane \
	,abz.AddresseeBank as MainAddresseeBank_zjebane,sbc.SenderBank as MainSenderBankByCount,abc.AddresseeBank as MainAddresseeBankByCount \
    ,COALESCE(rV.IsDepositingSalary,0) AS IsDepositingSalary,rV.AccountDurationTime,COALESCE(rV.HasStandingOrder,0) AS HasStandingOrder \
	,COALESCE(rV.HasCancelledStandingOrders,0) AS HasCancelledStandingOrders,COALESCE(rV.SumOfOperations,0) AS SumOfOperations \
	,COALESCE(rV.NumberOfOperations,0) AS NumberOfOperations,COALESCE(rV.ClientPaymentType,'LackOfTransfer') AS ClientPaymentType \
	,COALESCE(rV.AvgSumOutgoingOperations,0) AS AvgSumOutgoingOperations,COALESCE(rV.MaxSumOutgoingOperationsRecently,0) AS MaxSumOutgoingOperationsRecently \
    ,COALESCE(rV.SumOutgoingOpsRatio,0) AS SumOutgoingOpsRatio,COALESCE(rV.HadBranchOperationRecently,0) AS HadBranchOperationRecently \
FROM seip \
LEFT OUTER JOIN	rorVars rV \
ON seip.ClientID = rV.ClientID and seip.ReportingDate = rV.ReportingDate \
LEFT OUTER JOIN	senderBanks sb \
ON seip.ClientID = sb.ClientID and seip.ReportingDate = sb.ReportingDate AND sb.BankRnk = 1 \
LEFT OUTER JOIN addresseeBanks ab \
ON seip.ClientID = ab.ClientID and seip.ReportingDate = ab.ReportingDate AND ab.BankRnk = 1 \
LEFT OUTER JOIN senderBanks_zjebane sbz \
ON seip.ClientID = sbz.ClientID and seip.ReportingDate = sbz.ReportingDate AND sbz.BankRnk = 1 \
LEFT OUTER JOIN	addresseeBanks_zjebane abz \
ON seip.ClientID = abz.ClientID and seip.ReportingDate = abz.ReportingDate AND abz.BankRnk = 1 \
LEFT OUTER JOIN senderBanks sbc \
ON seip.ClientID = sbc.ClientID and seip.ReportingDate = sbc.ReportingDate AND sbc.BankRnkCnt = 1 \
LEFT OUTER JOIN	addresseeBanks abc \
ON seip.ClientID = abc.ClientID and seip.ReportingDate = abc.ReportingDate AND abc.BankRnkCnt = 1 \
")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/CHURN/SparkSQL/ChurnDepoVariables"
ChurnDepoVariables.write.mode('overwrite').parquet(HadoopLink2 + "ChurnDepoVariables")
