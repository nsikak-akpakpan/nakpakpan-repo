#/bin/sh

#script to test data prep procedures for demobank

pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnAccountLimitsVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnClientVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnComplaintsVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnCrbVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnChannelVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnExcessPayemntVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnInboundCallVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnLoanApplicationVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildCashLoanForRandomSample.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnDepoVariables.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildChurnDependentVariableLast3MWindowsChurnIn3M.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
#pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildVariablePredictiveness.kjb /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
#pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildWOEInput.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/
