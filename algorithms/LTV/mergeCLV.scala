// Joins Churn Input table with Churn output (ChurnScore) to produce CLV input dataset


// Author: Nsikak



package loadTables

// import 
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrame

import java.util.Date
import java.util.Calendar

import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.monotonicallyIncreasingId

import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI


// MAIN
object main 
{
  
  
  
  //////////////////////////////////////////////////////////////////////////
  // Cofiguration
  var sqlContext: org.apache.spark.sql.SQLContext = null
  var sc: org.apache.spark.SparkContext = null;
  val submissionFlag = true // Option for submission. Make true when spark-submitting
  var startTime: Date = null
  
//  val inChurnInputFileName = "hdfs://10.82.187.10:8020/hadoop/hdfs/PROCESSING/analytics_input/CLV/CLVWOEInput_parquet"
//  val inChurnOutputFileName = "hdfs://10.82.187.10:8020/hadoop/hdfs/OUT/ChurnCLV_CVEnsembleOutput_parquet"
//  val outCLVInputFileName = "hdfs://10.82.187.10:8020/data/modeling/churn/INPUT_CLV"
//  val outSQLTableName = "C360.visuals.CHURN_OUTPUT"
    
  var inChurnInputFileName: String = null
  var inChurnOutputFileName: String = null
  var outCLVInputFileName: String = null
  var outSQLTableName: String = null
    
  
  
  //////////////////////////////////////////////////////////////////////////
  // Main function
  def main(args: Array[String]) 
  {
    
    // Log
    startTime = Calendar.getInstance().getTime()
    println(startTime.toLocaleString() + " INFO main: Started")

    
    // // Initialize configuration
    Initialise(args)   
      
    
    
    // // Get data 
    val dfChurnInput = sqlContext.read.load(inChurnInputFileName)
    val dfChurnOutput = sqlContext.read.load(inChurnOutputFileName)
    dfChurnInput.cache()
    dfChurnOutput.cache()
    dfChurnInput.registerTempTable("dfChurnInput")
    dfChurnOutput.registerTempTable("dfChurnOutput")
    
    
    // Control the counts 
    val dfChurnInputCount = dfChurnInput.count()
    val dfChurnOutputCount = dfChurnOutput.count()
    if (dfChurnInputCount == dfChurnOutputCount){
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Got the data from " + inChurnInputFileName + " and " + inChurnOutputFileName)
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: lengths of datasets: " + dfChurnInputCount)
    }else{
      println(Calendar.getInstance().getTime().toLocaleString() + " ERROR main: lengths of datasets differ: " + dfChurnInputCount + " <> " + dfChurnOutputCount)
    }
    
    
    
    
    // // Save the output table into SQL
    
    // Connect
    val driver = "net.sourceforge.jtds.jdbc.Driver"
    val dbUrl = "jdbc:jtds:sqlserver://10.82.186.5;database=C360;user=pentaho;password=May2016pent";
    Class.forName(driver)
    val connection = DriverManager.getConnection(dbUrl)
 
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Connected to database")

    
    // Drop table if exists
    val statement = connection.createStatement()
    statement.executeUpdate("IF OBJECT_ID('"+ outSQLTableName + "', 'U') IS NOT NULL DROP TABLE " + outSQLTableName) 
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Dropped the table " + outSQLTableName)

   
    // Create table
    statement.executeUpdate("create table " +
                  outSQLTableName + 
                "([ClientID] [int] not NULL, " +
              	"[Month] [date] not NULL, " +
              	"[IsChurn] float NULL, " +
              	"[score] float NULL, " +
              	"[prediction] float NULL)"); 

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Created the table " + outSQLTableName)

    
    // Insert the dataframe into database
    val prop = new java.util.Properties()
    dfChurnOutput.write.mode(SaveMode.Append).jdbc(dbUrl, outSQLTableName, prop)
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Inserted into table " + outSQLTableName)

    
    // Join the data. 
    val dfJoined = sqlContext.sql("select ci.*, co.score as ChurnScore " + 
      "from dfChurnInput ci left join dfChurnOutput co " + 
      "on ci.ClientID = co.ClientID and ci.Month = co.Month")
    dfJoined.printSchema()
    dfJoined.cache()
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Joined the data")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Length of the joined the data = " + dfJoined.count())

  
    
    // // Record results into database
    val fsCLV:FileSystem = FileSystem.get(new URI(outCLVInputFileName), sc.hadoopConfiguration);
    fsCLV.delete(new Path(outCLVInputFileName), true) // true for recursive
    dfJoined.save(outCLVInputFileName) 
    FileUtil.chmod(outCLVInputFileName, "999", true)
      
      
    
    
     // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: Inserted into table " + outCLVInputFileName)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: finished")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")
       
  }
   
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to initialise all that needed
  def Initialise(args: Array[String]) = 
  {
        
    // Suppress the logs
    if (!submissionFlag) 
    {
      Logger.getLogger("org").setLevel(Level.OFF)  // Option for submission
    }
    
    // Context configuration
    var conf:SparkConf = null
    if (!submissionFlag) 
    {
      conf = new SparkConf().setAppName("SparkSQL_ETL").setMaster("local") // Option for local execution
    }
    else
    {
      conf = new SparkConf().setAppName("SparkSQL_ETL"); // Option for submission
    }
    
    // Spark configuration
    conf.set("spark.io.compression.codec", "lzf")
    conf.set("spark.speculation","true")
    conf.set("spark.hadoop.validateOutputSpecs", "false") 
    //conf.set("spark.executor.memory", "6154m") 
    //conf.set("spark.driver.memory", "3g") 
    //conf.set("spark.executor.cores", "8") 
    //conf.set("spark.cores.max", "infinite")
    //conf.set("spark.yarn.driver.memoryOverhead", "1024")
    conf.set("spark.sql.shuffle.partitions", "36")
    conf.set("spark.driver.maxResultSize", "16000m")
    
    // Rewrite the file if exists
    conf.set("spark.hadoop.validateOutputSpecs", "false") 
    
    // Contexts
    sc = new SparkContext(conf);
    sqlContext = new org.apache.spark.sql.SQLContext(sc)  
    
    // FilePaths
    inChurnInputFileName = args(0)
    inChurnOutputFileName = args(1)
    outCLVInputFileName = args(2)
    outSQLTableName = args(3)
     
  }
 
  
 
}


    