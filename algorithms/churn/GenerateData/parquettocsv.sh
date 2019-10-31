#/bin/sh

#$1 = /hadoop/hdfs/demobank/INPUTPARQUET/appl/RiskCategories
#$2 = /hadoop/hdfs/demobank/INPUTCSV/appl/RiskCategories
#$3 = /tmp/demobank/ingest/appl_RiskCategories.csv

pyspark --packages com.databricks:spark-csv_2.10:1.4.0 parquettocsv.py $1_parquet $2_csv
hadoop fs -mv $2_csv/part-00000 $2.csv
hadoop fs -copyToLocal $2.csv $3
hadoop fs -rmr $2_csv
