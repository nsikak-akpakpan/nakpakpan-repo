import sys
from pyspark import SparkContext, SparkConf, StorageLevel, RDD
from pyspark.sql import SQLContext, Row, HiveContext
from pyspark.sql.window import Window
from pyspark.sql import functions as psf

input = sys.argv[1]
filter = sys.argv[2]
output1 = sys.argv[3]
csvout = sys.argv[4]

#conf.setAppName("GenerateData")

sc = SparkContext()
sq = SQLContext(sc)
hq = HiveContext(sc)

###inputpath = "hdfs://10.82.187.10:8020/hadoop/hdfs/INPUTPARQUET/"
##filterfile = "hdfs://10.82.187.10:8020//hadoop/hdfs/INPUTPARQUET/dbo/SubselectionOfClients_parquet"
###outputpath = "hdfs://10.82.187.10:8020/hadoop/hdfs/demobank/INPUTPARQUET/"
###csvout="hdfs://10.82.187.10:8020/hadoop/hdfs/demobank/csv/"

input = sq.read.parquet(input)
input.registerTempTable("input")

filter = sq.read.parquet(filter)
filter.registerTempTable("filter")

output= sq.sql("select * from input INNER JOIN (SELECT ClientID as cd FROM filter) FOO on input.ClientID=FOO.cd")
output.drop("cd")
output.write.mode('overwrite').parquet(output1)
#output.write.format("com.databricks.spark.csv").option(header='true').save(csvout)


