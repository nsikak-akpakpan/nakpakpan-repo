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

ChurnAccountLimitsVariables   = hq.read.parquet(HadoopLink + "var/ChurnAccountLimitsVariables_parquet").persist()
ChurnAccountLimitsVariables.registerTempTable("ChurnAccountLimitsVariables")

ChurnChannelVariables   = hq.read.parquet(HadoopLink + "var/ChurnChannelVariables_parquet").persist()
ChurnChannelVariables.registerTempTable("ChurnChannelVariables")

ChurnClientVariables   = hq.read.parquet(HadoopLink + "var/ChurnClientVariables_parquet").persist()
ChurnClientVariables.registerTempTable("ChurnClientVariables")

ChurnComplaintsVariables   = hq.read.parquet(HadoopLink + "var/ChurnComplaintsVariables_parquet").persist()
ChurnComplaintsVariables.registerTempTable("ChurnComplaintsVariables")

ChurnCrbVariables   = hq.read.parquet(HadoopLink + "var/ChurnCrbVariables_parquet").persist()
ChurnCrbVariables.registerTempTable("ChurnCrbVariables")

ChurnDepoVariables   = hq.read.parquet(HadoopLink + "var/ChurnDepoVariables_parquet").persist()
ChurnDepoVariables.registerTempTable("ChurnDepoVariables")

ChurnExcessPayemntVariables   = hq.read.parquet(HadoopLink + "var/ChurnExcessPayemntVariables_parquet").persist()
ChurnExcessPayemntVariables.registerTempTable("ChurnExcessPayemntVariables")

ChurnInboundCallVariables   = hq.read.parquet(HadoopLink + "var/ChurnInboundCallVariables_parquet").persist()
ChurnInboundCallVariables.registerTempTable("ChurnInboundCallVariables")

ChurnLoanApplicationVariables   = hq.read.parquet(HadoopLink + "var/ChurnLoanApplicationVariables_parquet").persist()
ChurnLoanApplicationVariables.registerTempTable("ChurnLoanApplicationVariables")

CashLoanForRandomSample   = hq.read.parquet(HadoopLink + "var/CashLoanForRandomSample_parquet").persist()
CashLoanForRandomSample.registerTempTable("CashLoanForRandomSample")

ChurnDependentVariable   = hq.read.parquet(HadoopLink + "var/ChurnDependentVariable_parquet").persist()
ChurnDependentVariable.registerTempTable("ChurnDependentVariable")

ClientContractDateMapping = hq.read.parquet(HadoopLink + "dict/ClientContractDateMapping_parquet").persist()
ClientContractDateMapping.registerTempTable("ClientContractDateMapping")

predictors1 = hq.sql("\
select b.ChurnID,a.* \
from ChurnAccountLimitsVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate \
where b.ChurnID is not null")
predictors1.registerTempTable("predictors1")

predictors2 = hq.sql("\
select b.ChurnID,a.* \
from ChurnChannelVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate \
where b.ChurnID is not null")
predictors2.registerTempTable("predictors2")

predictors3 = hq.sql("\
select b.ChurnID,a.* \
from ChurnClientVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate \
where b.ChurnID is not null")
predictors3.registerTempTable("predictors3")

predictors4 = hq.sql("\
select b.ChurnID,a.* \
from ChurnExcessPayemntVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate and a.ContractID=b.ContractID \
where b.ChurnID is not null")
predictors4.registerTempTable("predictors4")

predictors5 = hq.sql("\
select b.ChurnID,a.* \
from ChurnInboundCallVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate \
where b.ChurnID is not null")
predictors5.registerTempTable("predictors5")

predictors6 = hq.sql("\
select b.ChurnID,a.* \
from ChurnLoanApplicationVariables a \
left join ClientContractDateMapping b \
on a.ContractID=b.ContractID and a.ReportingDate=b.ReportingDate and a.ClientID=b.ClientID \
where b.ChurnID is not null")
predictors6.registerTempTable("predictors6")

predictors7 = hq.sql("\
select b.ChurnID,a.* \
from ChurnComplaintsVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate \
where b.ChurnID is not null")
predictors7.registerTempTable("predictors7")

cashloansample = hq.sql("\
select b.ChurnID,a.* \
from CashLoanForRandomSample a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate and a.ContractID=b.ContractID \
where b.ChurnID is not null")
cashloansample.registerTempTable("cashloansample")

avgCRB = hq.sql("\
SELECT \
	vn.ReportingDate, \
	AVG(vn.ActiveToAllCashLoansRatio) AS AvgActiveToAllCashLoansRatio, \
	AVG(vn.ActiveToAllCreditsRatio) AS AvgActiveToAllCreditsRatio, \
	AVG(vn.ActiveEBToAllActiveCashLoansRatio) AS AvgActiveEBToAllActiveCashLoansRatio, \
	AVG(vn.ActiveEBToAllActiveCreditsRatio) AS AvgActiveEBToAllActiveCreditsRatio \
FROM ChurnCrbVariables vn \
GROUP BY vn.ReportingDate")
avgCRB.registerTempTable("avgCRB")

predictors8 = hq.sql("\
select \
    b.ChurnID,a.*,avgCRB.AvgActiveToAllCashLoansRatio,avgCRB.AvgActiveToAllCreditsRatio \
    ,avgCRB.AvgActiveEBToAllActiveCashLoansRatio,avgCRB.AvgActiveEBToAllActiveCreditsRatio \
from ChurnCrbVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate \
left join avgCRB \
on a.ReportingDate=avgCRB.ReportingDate \
where b.ChurnID is not null")
predictors8.registerTempTable("predictors8")

predictors9 = hq.sql("\
select \
    b.ChurnID,a.* \
from ChurnDepoVariables a \
left join ClientContractDateMapping b \
on a.ClientID=b.ClientID and a.ReportingDate=b.ReportingDate \
where b.ChurnID is not null")
predictors9.registerTempTable("predictors9")

WOEInput = hq.sql("\
select \
    predictors2.ChurnID \
    ,cashloansample.Tenor,cashloansample.MOB,cashloansample.DPD,cashloansample.StartDate,cashloansample.StartYear \
\
    ,predictors2.AvgNumLoginsRecently,predictors2.AvgNumLoginsOnLoginDaysRecently,predictors2.LoginActivityDiff \
    ,predictors2.DaysSinceLastLogin,predictors2.DaysSinceLastTransfer,predictors2.AvgNumTransfersRecently \
    ,predictors2.NumDaysDoladRecently,cast(predictors2.HasOpenedDepositRecently as varchar(1)) as HasOpenedDepositRecently \
    ,cast(predictors2.HasClosedDepositRecently as varchar(1)) as HasClosedDepositRecently \
    ,cast(predictors2.HasPayedCCRecently as varchar(1)) as HasPayedCCRecently \
    ,cast(predictors2.HasActivatedDebitCardRecently	 as varchar(1)) as HasActivatedDebitCardRecently \
    ,cast(predictors2.HasReachedLoginsLimitRecently	 as varchar(1)) as HasReachedLoginsLimitRecently \
    ,cast(predictors2.HasNewOrderRecently as varchar(1)) as HasNewOrderRecently \
    ,predictors2.CurrentMonthVisits,predictors2.LastMonthVisits,predictors2.YearlyVisitAvgPerMonth,predictors2.CurrentMonthVisitsCO \
    ,predictors2.LastMonthVisitsCO,predictors2.YearlyVisitCOAvgPerMonth,predictors2.CurrentMonthVisitsSO \
    ,predictors2.LastMonthVisitsSO,predictors2.YearlyVisitSOAvgPerMonth,predictors2.PhisicalTokenUsage \
    ,predictors2.JavaTokenUsage,cast(predictors2.HasActiveBE as varchar(1)) as HasActiveBE,predictors2.MonthsSinceFirstBELogin \
\
    ,predictors1.CurrentLimit,predictors1.InitialLimit,predictors1.LimitReached,predictors1.InitialLimitEqualsCurrent \
    ,predictors1.LimitReachedInLast3Months,predictors1.LimitChangedInLast3Months,cast(predictors3.IncomeTypeID    as varchar(20)) as IncomeTypeID \
\
    ,cast(predictors3.MaritalStatusID as varchar(20)) as MaritalStatusID,predictors3.age,cast(predictors3.IsBankEmployee as varchar(1)) as IsBankEmployee \
    ,cast(predictors3.IsOnBlackList as varchar(1)) as IsOnBlackList,cast(predictors3.WasEverOnBlackList  as varchar(1)) as WasEverOnBlackList \
\
    ,predictors4.ExcessPaymentAmmountCurrent,predictors4.NumberOfExcessPayments3M,predictors4.ExcessPaymentAmmount3M \
    ,CASE WHEN predictors4.NumberOfExcessPayments6M is not null then predictors4.NumberOfExcessPayments6M else -10 end AS NumberOfExcessPayments6M \
    ,case when predictors4.ExcessPaymentAmmount6M is not null then predictors4.ExcessPaymentAmmount6M else -100 end AS ExcessPaymentAmmount6M \
    ,case when predictors4.NumberOfExcessPaymentsHist is not null then predictors4.NumberOfExcessPaymentsHist else -10 end AS NumberOfExcessPaymentsHist \
    ,predictors4.HasExcessPaymentAmmountHist \
    ,CAST(predictors4.HasExcessPaymentAmmountHist as varchar(2)) AS HasExcessPaymentAmmountHistString \
    ,CAST(CASE WHEN COALESCE(predictors4.NumberOfExcessPayments6M , 0) > 2 THEN 2 ELSE COALESCE(predictors4.NumberOfExcessPayments6M,0) END AS VARCHAR(4)) AS NumExcessPayments6MStr \
\
    ,predictors5.PrematurePayment1M,predictors5.Windication1M,predictors5.Complaints1M,predictors5.Console1M \
    ,predictors5.NewProducts1M,predictors5.AVGWaitTime1M,predictors5.AVGTalkTime1M \
    ,cast(predictors5.IsAnswered1M as varchar(1)) as IsAnswered1M,cast(predictors5.IsOffered1M  as varchar(1)) as IsOffered1M \
    ,predictors5.PrematurePayment3M,predictors5.Windication3M,predictors5.Complaints3M,predictors5.Console3M \
    ,predictors5.NewProducts3M,predictors5.AVGWaitTime3M,predictors5.AVGTalkTime3M \
    ,cast(predictors5.IsAnswered3M as varchar(1)) as IsAnswered3M,cast(predictors5.IsOffered3M as varchar(1)) as IsOffered3M \
\
    ,predictors6.NumberOfAllLoanApplications,predictors6.HasLoanApplication,predictors6.HasRiskSegmentChange \
    ,predictors6.HasMartialStatusChange,predictors6.IncomeChanges,predictors6.HadBankRejectedApplRecently \
    ,predictors6.HadClientRejectedApplRecently,predictors6.EducationID,predictors6.Income,predictors6.RiskSegment \
    ,predictors6.KLNSegment,predictors6.MonthsSinceApplication,predictors6.SavingsToBalanceVer4,predictors7.NumberOfQueryThreads \
    ,predictors7.NumberOfComplaintsThreads,predictors7.NumberOfOtherThreads,predictors7.NumberOfPendingThreads \
    ,predictors7.NumberOfRejectedThreads,predictors7.NumberOFAcceptedThreads \
\
    ,predictors8.MonthsSinceLastBikCredit \
    ,CASE WHEN predictors8.ActiveMortgageInOtherBanksCount is not null then predictors8.ActiveMortgageInOtherBanksCount else 0 end AS ActiveMortgageInOtherBanksCount \
    ,CASE WHEN predictors8.ActiveMortgageInOtherBanksCount>0 then 1 else 0 end AS HasActiveMortgageInOtherBanksCount \
    ,case when predictors8.ActiveCashLoanInOtherBanksCount is not null then predictors8.ActiveCashLoanInOtherBanksCount else 0 end AS ActiveCashLoanInOtherBanksCount \
    ,case when predictors8.ActiveCashLoanInOtherBanksCount>0 then 1 else 0 end AS HasActiveCashLoanInOtherBanksCount \
    ,case when predictors8.ActiveCreditInOtherBankCount is not null then predictors8.ActiveCreditInOtherBankCount else 0 end AS ActiveCreditInOtherBankCount \
    ,case when predictors8.ActiveCreditNotDefaultedInOtherBankCount is not null then predictors8.ActiveCreditNotDefaultedInOtherBankCount else 0 end AS ActiveCreditNotDefaultedInOtherBankCount \
    ,case when predictors8.SumActiveForeignBalance is not null then predictors8.SumActiveForeignBalance else 0 end AS SumActiveForeignBalance \
    ,case when predictors8.ChurnedCreditHistoryCount is not null then predictors8.ChurnedCreditHistoryCount else 0 end AS ChurnedCreditHistoryCount \
\
    ,COALESCE(predictors8.ActiveToAllCashLoansRatio, predictors8.AvgActiveToAllCashLoansRatio) AS ActiveToAllCashLoansRatio \
    ,COALESCE(predictors8.ActiveToAllCreditsRatio, predictors8.AvgActiveToAllCreditsRatio) AS ActiveToAllCreditsRatio \
    ,COALESCE(predictors8.ActiveEBToAllActiveCashLoansRatio, predictors8.AvgActiveEBToAllActiveCashLoansRatio) AS ActiveEBToAllActiveCashLoansRatio \
    ,COALESCE(predictors8.ActiveEBToAllActiveCreditsRatio, predictors8.AvgActiveEBToAllActiveCreditsRatio) AS ActiveEBToAllActiveCreditsRatio \
    ,case when predictors8.InstallmentDiffCount is not null then predictors8.InstallmentDiffCount else 0 end AS InstallmentDiffCount \
    ,case when predictors8.BalanceDifferance1 is not null then predictors8.BalanceDifferance1 else 0 end AS BalanceDifferance1 \
    ,case when predictors8.BalanceDifferance2 is not null then predictors8.BalanceDifferance2 else 0 end AS BalanceDifferance2 \
    ,case when predictors8.QueriesPerRecentMth is not null then predictors8.QueriesPerRecentMth else 0 end AS QueriesPerRecentMth \
    ,CAST((CASE WHEN predictors8.QueriesPerRecentMth >= 1 THEN '1' WHEN predictors8.QueriesPerRecentMth = 0 THEN '0' ELSE '-666' END) AS VARCHAR(10)) AS QueriesPerRecentMthCat \
    ,predictors8.QueriesInLast3M,predictors8.QueriesInLast1M,predictors8.QueriesAtApplication,predictors9.NumNonZeroAccounts \
\
    ,predictors9.NumAccounts,predictors9.HasOpenedAccountRecently,predictors9.HasClosedAccountRecently,predictors9.TotalRORBalance \
    ,predictors9.TotalBalanceDiffVsRecently,predictors9.TotalTransferAmount,predictors9.TotalTransferCount,predictors9.SameNameSurnameAmount \
    ,predictors9.SameNameSurnameCount,predictors9.MainSenderBank,predictors9.MainAddresseeBank,predictors9.MainSenderBank_zjebane \
    ,predictors9.MainAddresseeBank_zjebane,predictors9.MainSenderBankByCount,predictors9.MainAddresseeBankByCount \
    ,case when predictors9.AccountDurationTime is not null then predictors9.AccountDurationTime else -10 end AS AccountDurationTime \
    ,predictors9.HasStandingOrder,predictors9.SumOfOperations,predictors9.NumberOfOperations,predictors9.ClientPaymentType \
    ,predictors9.AvgSumOutgoingOperations,predictors9.MaxSumOutgoingOperationsRecently,predictors9.SumOutgoingOpsRatio \
    ,predictors9.HadBranchOperationRecently,CAST(predictors9.HadBranchOperationRecently AS varchar(2)) AS HadBranchOperationRecentlyString \
    ,CAST(predictors9.IsDepositingSalary as varchar(2)) AS IsDepositingSalary \
    ,cdv.Target \
from ChurnDependentVariable cdv \
left join predictors2 ON cdv.ChurnID = predictors2.ChurnID \
left join predictors1 on predictors2.ChurnID=predictors1.ChurnID \
left join predictors3 on predictors2.ChurnID=predictors3.ChurnID \
left join predictors4 on predictors2.ChurnID=predictors4.ChurnID \
left join predictors5 on predictors2.ChurnID=predictors5.ChurnID \
left join predictors6 on predictors2.ChurnID=predictors6.ChurnID \
left join predictors7 on predictors2.ChurnID=predictors7.ChurnID \
left join cashloansample on predictors2.ChurnID=cashloansample.ChurnID \
left join predictors8 on predictors2.ChurnID=predictors8.ChurnID \
left join predictors9 on predictors2.ChurnID=predictors9.ChurnID \
WHERE cdv.Tag = 'Last3MWindowsChurnIn3M' \
")

###HadoopLink2 = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUT/var/SparkSQL/WOEInput"
WOEInput.write.mode('overwrite').parquet(HadoopLink2 + "WOEInput")