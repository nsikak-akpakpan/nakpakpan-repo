#/bin/sh 

./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/contr/DepositHistory /hadoop/hdfs/demobank/INPUTCSV/contr/DepositHistory /tmp/demobank/ingest/contr_DepositHistory.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/contr/AccountProperties /hadoop/hdfs/demobank/INPUTCSV/contr/AccountProperties /tmp/demobank/ingest/contr_AccountProperties.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/contr/CreditHistory /hadoop/hdfs/demobank/INPUTCSV/contr/CreditHistory /tmp/demobank/ingest/contr_CreditHistory.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/cli/Client /hadoop/hdfs/demobank/INPUTCSV/cli/Client /tmp/demobank/ingest/cli_Client.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/cli/ClientBlackList /hadoop/hdfs/demobank/INPUTCSV/cli/ClientBlackList /tmp/demobank/ingest/cli_ClientBlackList.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/chan/ComplaintLog /hadoop/hdfs/demobank/INPUTCSV/chan/ComplaintLog /tmp/demobank/ingest/chan_ComplaintLog.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/crb/CreditBureauRaport /hadoop/hdfs/demobank/INPUTCSV/crb/CreditBureauRaport /tmp/demobank/ingest/crb_CreditBureauRaport.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/crb/CreditBureauRequests /hadoop/hdfs/demobank/INPUTCSV/crb/CreditBureauRequests /tmp/demobank/ingest/crb_CreditBureauRequests.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/crb/CreditBureau /hadoop/hdfs/demobank/INPUTCSV/crb/CreditBureau /tmp/demobank/ingest/crb_CreditBureau.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/chan/BranchVisitLog /hadoop/hdfs/demobank/INPUTCSV/chan/BranchVisitLog /tmp/demobank/ingest/chan_BranchVisitLog.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/chan/InternetBankingLog /hadoop/hdfs/demobank/INPUTCSV/chan/InternetBankingLog /tmp/demobank/ingest/chan_InternetBankingLog.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/chan/LoggingContract /hadoop/hdfs/demobank/INPUTCSV/chan/LoggingContract /tmp/demobank/ingest/chan_LoggingContract.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/chan/TokenUsageLog /hadoop/hdfs/demobank/INPUTCSV/chan/TokenUsageLog /tmp/demobank/ingest/chan_TokenUsageLog.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/dict/TypeOfAction /hadoop/hdfs/demobank/INPUTCSV/dict/TypeOfAction /tmp/demobank/ingest/dict_TypeOfAction.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/contr/Transactions /hadoop/hdfs/demobank/INPUTCSV/contr/Transactions /tmp/demobank/ingest/contr_Transactions.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/chan/InboundCallLog /hadoop/hdfs/demobank/INPUTCSV/chan/InboundCallLog /tmp/demobank/ingest/chan_InboundCallLog.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/chan/InboundCallReason /hadoop/hdfs/demobank/INPUTCSV/chan/InboundCallReason /tmp/demobank/ingest/chan_InboundCallReason.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/appl/CreditApplication /hadoop/hdfs/demobank/INPUTCSV/appl/CreditApplication /tmp/demobank/ingest/appl_CreditApplication.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/appl/RiskCategories /hadoop/hdfs/demobank/INPUTCSV/appl/RiskCategories /tmp/demobank/ingest/appl_RiskCategories.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/seg/ClientSegment /hadoop/hdfs/demobank/INPUTCSV/seg/ClientSegment /tmp/demobank/ingest/seg_ClientSegment.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/score/ClientScore /hadoop/hdfs/demobank/INPUTCSV/score/ClientScore /tmp/demobank/ingest/score_ClientScore.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/contr/AggTransactions /hadoop/hdfs/demobank/INPUTCSV/contr/AggTransactions /tmp/demobank/ingest/contr_AggTransactions.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/var/CashLoanForRandomSample /hadoop/hdfs/demobank/INPUTCSV/var/CashLoanForRandomSample /tmp/demobank/ingest/var_CashLoanForRandomSample.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/dict/ClientContractDateMapping /hadoop/hdfs/demobank/INPUTCSV/dict/ClientContractDateMapping /tmp/demobank/ingest/dict_ClientContractDateMapping.csv
./parquettocsv.sh /hadoop/hdfs/demobank/INPUTPARQUET/contr/SaleOfCredits /hadoop/hdfs/demobank/INPUTCSV/contr/SaleOfCredits /tmp/demobank/ingest/contr_SaleOfCredits.csv

