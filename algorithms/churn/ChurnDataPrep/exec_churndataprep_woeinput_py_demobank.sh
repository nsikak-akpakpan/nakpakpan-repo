#/bin/sh

#run this after BuildVariablePredictiveness.kjb

pySpark --packages com.databricks:spark-csv_2.10:1.4.0  BuildWOEInput.py /hadoop/hdfs/demobank/INPUTPARQUET/  /hadoop/hdfs/demobank/INPUT/var/CHURN/SparkSQL/