/* 
BEHAVIORAL MICROSEGMENTATION OF CLIENT*MONTH HISTORICAL DATA
USES PRECALIBRATED MODEL


LAST RUN TIME
Cluster: 1h
*/

package MicroSegmentation

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

// Uncategorised
import scala.util.Random
import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import java.text.DateFormat
 
// sql connectivity
import java.sql.DriverManager
import java.sql.Connection

// ML
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.regression.LabeledPoint

// Java
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import breeze.linalg._

// MAIN
object main1 
{
  // Configuration
  val targetColNames = Array("MonthlyRevenue", "ChurnScore") 
  val targetColumn = "TargetCat"
  val keyColumns = Array("ClientID", "Month") 
  val busSegmentName = "BusSegment"
  val isNewJoinerColName = "IsNewJoiner"
  val isChurnColName = "IsChurn"
  
  var inputDataLength: Int = 0
  var sqlContext: org.apache.spark.sql.SQLContext = null
  var sc: org.apache.spark.SparkContext = null;
  
//  val SegmentDataOutAbsPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/D_SEGMENTS"
//  val SegmentationOutAbsPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/F_SEGMENTS"
//  val CLVInputAbsPath = "hdfs://10.82.187.10:8020/data/modeling/churn/INPUT_CLV"
//  val SegmentationModelAbsPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/M_SEGMENTATION_MODEL"
//  
//  val SegmentDataOutRelPath = "/data/modeling/CLV/D_SEGMENTS"
//  val SegmentationOutRelPath = "/data/modeling/CLV/F_SEGMENTS"
//  val CLVInputRelPath = "/data/modeling/churn/INPUT_CLV"
//  val SegmentationModelRelPath = "/data/modeling/CLV/M_SEGMENTATION_MODEL"
  
  var SegmentDataOutPath:String = null
  var SegmentationOutPath:String = null
  var CLVInputPath:String = null
  var SegmentationModelPath:String = null
  var actualDate: String = null
  var startTime: Date = null
  var dateTimeFormatter: DateFormat = null
  
  val submissionFlag = true // !! Option for submission. Make true when spark-submitting !!
  val correlThreshold = 0.7
  val numColumnsUse = 20
  val saveFlag = true  // Option for output
  

  def main(args: Array[String]) 
  {

    // Initialize configuration
    Initialise(args)   

    // Get the data 
    val allData = getCLVData()

    // Filter columns by different statistical methods
    val dataFilt = clearData(allData)
    
    // Separate the special segments from the data
    val (dataWithoutSpSegments, dataWithSpSegments) = separateSpecialSegments(dataFilt)
        
    // Make prediction by Classification tree model
    val dataWSegments = classificationTreesByBusinessSegments(dataWithoutSpSegments)
    
    // Add data with the special segments back to the data
    val dataWSegmAndSpSegm = joinSpecialSegmentsData(dataWSegments, dataWithSpSegments)
     
    // Prepare outputs and save them 
    calculateSegmentAggregates(dataWSegmAndSpSegm)  
    
     // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: finished")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")
       
  }
  

    
  //////////////////////////////////////////////////////////////////////////
  // Function to join special segments back into data 
  def joinSpecialSegmentsData(dataMS: DataFrame, dataSS: DataFrame) 
  : DataFrame = 
  {
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO joinSpecialSegmentsData: started")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO joinSpecialSegmentsData: from beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")
 
    
    // Select the needed columns from dataMS
    dataMS.registerTempTable("dataMS")
    val dataMSSelected = sqlContext.sql("select " + 
        keyColumns.mkString(", ") + ", " +
        targetColNames.mkString(", ") + ", " +
        busSegmentName + ", " + 
        "BLeafID " +
        "from dataMS ")
    
    // Add the MS columns to the special segments DataFrame
    dataSS.registerTempTable("dataSS")
    val forecastSpSegmData = sqlContext.sql("select " +
        keyColumns.mkString(", ") + ", " +
        targetColNames.mkString(", ") + ", " +
        busSegmentName + ", " + 
        busSegmentName + " as BLeafID " +
      " from dataSS")
      
    // Join the data  
    var joinedData = forecastSpSegmData.unionAll(dataMSSelected)

    // Log
    println("joinedData.count() = " + joinedData.count())
   
    return(joinedData)
  }
   
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to separate special segments from data 
  def separateSpecialSegments(dataAll: DataFrame) 
  :(DataFrame, DataFrame) = 
  {
   
    // Separate the segments
    dataAll.registerTempTable("dataAll")
    val dataWithoutSpSegments = sqlContext.sql("select * from dataAll " +
        "where " + isChurnColName + " <> true " + 
        "and " + isNewJoinerColName + " <> true ")
    dataWithoutSpSegments.cache()
        
    val dataWithSpSegments = sqlContext.sql("select * from dataAll " +
        "where " + isChurnColName + " = true " + 
        "or " + isNewJoinerColName + " = true ")
    dataWithSpSegments.cache()
    
    // Update BusSegment by corresponding leave segment name
    dataWithSpSegments.registerTempTable("dataWithSpSegments")
    val dataWithSpSegmentsBSUpdated = sqlContext.sql("select *, case " + 
        "when " + isChurnColName + "= true then '" + isChurnColName + "' " +
        "when " + isNewJoinerColName + "= true then '" + isNewJoinerColName + "' " +
        "else " + busSegmentName + " " + 
        "end as BusSegmentUpdated from dataWithSpSegments")
        .drop(busSegmentName)
        .withColumnRenamed("BusSegmentUpdated", busSegmentName)
    dataWithSpSegmentsBSUpdated.cache()
        
    // Log
    println("dataWithoutSpSegments.show():")
    dataWithoutSpSegments.show(5)
    println("dataWithoutSpSegments.count() = " + dataWithoutSpSegments.count() )
    println("dataWithSpSegmentsBSUpdated.show():")
    dataWithSpSegmentsBSUpdated.show(5)
    println("dataWithSpSegmentsBSUpdated.count() = " + dataWithSpSegmentsBSUpdated.count() )
    
    return(dataWithoutSpSegments, dataWithSpSegmentsBSUpdated)
  }
  
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to initialize all that needed
  def Initialise(args: Array[String]) = 
  {
        
    // Log
    startTime = new Date()
    dateTimeFormatter =  DateFormat.getDateTimeInstance()
    
    println(dateTimeFormatter.format(startTime) + " INFO main1: Started")
    
    // Get end of month date  
    val aTime = Calendar.getInstance()
    aTime.set(Calendar.DAY_OF_MONTH, aTime.getActualMaximum(Calendar.DAY_OF_MONTH))
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    actualDate = formatter.format(aTime.getTime())
        
    // Suppress the logs
    if (!submissionFlag) 
    {
      Logger.getLogger("org").setLevel(Level.OFF)  // Option for submission
    }
    
    // Context configuration
    var conf:SparkConf = null
    if (!submissionFlag) 
    {
      conf = new SparkConf().setAppName("CLV").setMaster("local") // Option for local execution
    }
    else
    {
      conf = new SparkConf().setAppName("CLV"); // Option for submission
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
    
    // File paths

    SegmentDataOutPath = args(0)
    SegmentationOutPath = args(1)
    CLVInputPath = args(2)
    SegmentationModelPath = args(3)
     
  }
 
  
  
  //////////////////////////////////////////////////////////////////////////
  // Get data function
  def getCLVData() 
  :DataFrame = 
  {

  val inputDF = sqlContext.read.load(CLVInputPath).coalesce(12).cache()  
    
    // Take only last 6 months of the data
    inputDF.registerTempTable("inputDF")
    var dataDF_CLV = sqlContext.sql("select * from inputDF where Month >= '2014-01-30'")
    dataDF_CLV.coalesce(36).cache()
    
    // TEMP DEBUG: 
    // dataDF_CLV = dataDF_CLV.sample(false, 0.01)
    
    val count = dataDF_CLV.count()
    println("Read in dataDF_CLV with row count = "  + count)
    return dataDF_CLV
  }
  

  
  //////////////////////////////////////////////////////////////////////////
  // Function to convert datetime columns to numeric type 
  def convertDateTime2Numeric(inData: DataFrame)
  : DataFrame = 
  {
    //Log
    println("\n" + dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO convertDateTime2Numeric: converting datetime to numeric")

    // Get list of types
    val types = inData.dtypes
      .filterNot(p => keyColumns.contains(p._1))
    
    var outData = inData.persist()
    
    // Loop over columns
    for (colNameType <- types){
       if (colNameType._2 == "TimestampType") {
         outData = outData
          .withColumn(colNameType._1 + "Long", outData(colNameType._1).cast("Long"))
          .drop(colNameType._1)
       }
    }
    
    outData.unpersist()
    inData.unpersist()

    // Log
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO convertDateTime2Numeric: finished")
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO convertDateTime2Numeric: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")

    return(outData)
  }
  
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to remove/replace null values in the data
  def treatNullValues(inData: DataFrame)
  : DataFrame = 
  {
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO treatNullValues: Started ")
    
    // Remove lines with nulls in target columns
    inData.registerTempTable("inData")
    val dataCleanTarget = sqlContext.sql("select * from inData where " + targetColNames(0) + " is not null and " +  targetColNames(1) + " is not null")
    
    // Log
    println("The null target records in dataset ")
    sqlContext.sql("select * from inData where " + targetColNames(0) + " is null or " +  targetColNames(1) + " is null").show()
    
    // Replace nulls in independent variables by an enourmously small number
    // TODO: Define the value to replace nulls basing on min values in data
    val dataNumericNotNull = dataCleanTarget.na.fill(-100000)
    val dataNumericNotNullNull = dataNumericNotNull.na.fill("NullValue")
    dataNumericNotNullNull.registerTempTable("dataNumericNotNull")

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO treatNullValues: Finished")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO treatNullValuess: Minimal/maximal ChurnScore = ")
    sqlContext.sql("select min(ChurnScore), max(ChurnScore) from dataNumericNotNull ").show()
    
    //Return
    return dataNumericNotNullNull
  }
  
  

  //////////////////////////////////////////////////////////////////////////
  // Function to clear the data
  def clearData(inData: DataFrame)
  : DataFrame = 
  {
    
    // Trim the BusSegment name
    val inDataTrim  = inData
      .withColumn("BusSegmentTrim", trim(column("BusSegment")))
      .drop("BusSegment")
      .withColumnRenamed("BusSegmentTrim", "BusSegment")
    
      
    // Convert the columns of timeStamp type into a double variable
    val dataNumeric = convertDateTime2Numeric(inDataTrim)
    

    // Treat the Null values
    val dataNumericNotNull = treatNullValues(dataNumeric)
    dataNumericNotNull.registerTempTable("dataNumericNotNull")
       
    
    // Index all the strings
    var outData = indexAllStrings(dataNumericNotNull)
    outData.persist()
    
    
    //Log
    println("\n" + dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO clearData: outData.count() = " + outData.count())
    println("\n" + dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO clearData: filtered data")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO clearData: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")
    
    return outData 
  }

    
  
  //////////////////////////////////////////////////////////////////////////
  // Function to index all string columns
  def indexAllStrings(inData: DataFrame)
  : DataFrame = 
  {
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO indexAllStrings: begin ")
    
    var outData = inData
    outData.persist()
   
    // Get list of types
    val types = outData.dtypes
      .filterNot(p => targetColNames.contains(p._1))
      .filterNot(p => keyColumns.contains(p._1))
      .filterNot(p => Array(targetColumn).contains(p._1))
      .filterNot(p => Array(busSegmentName).contains(p._1))
      .filterNot(p => Array(isNewJoinerColName).contains(p._1))
      .filterNot(p => Array(isChurnColName).contains(p._1))
    println("List of columns and their types:")
    println(types.mkString(", "))
    
    
    // Loop over columns
    for (colNameType <- types){
         
      // If type is string make it numeric
      if (colNameType._2 == "StringType") {
         val si = new StringIndexer()
           .setInputCol(colNameType._1)
           .setOutputCol(colNameType._1 + "StringLong")
           .fit(outData)
         outData = si
           .transform(outData)
           .drop(colNameType._1)
         println("Transformed " + colNameType._1 + " to " + colNameType._1 + "StringLong")
       }
    }
    
    outData.unpersist()

    println(Calendar.getInstance().getTime().toLocaleString() + " INFO indexAllStrings: end")
    
    // Return
    return outData
  }
 
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to prepare features for Decision Tree
  def prepareTreeFeaturesLabel(data: DataFrame)
  : DataFrame = 
  {
    
    // Define list of features
    val fullColumnList = data.columns;
    val featuresList = fullColumnList
      .filterNot( targetColNames.toSet)
      .filterNot(keyColumns.toSet)
      .filterNot(Array(targetColumn).toSet)
      .filterNot(Array(busSegmentName).toSet)
      .filterNot(Array(isNewJoinerColName).toSet)
      .filterNot(Array(isChurnColName).toSet)
    
    // Apply RFormula
    val RFormula = targetColumn + " ~ " + featuresList.mkString(" + ")
    println("RFormula to prepare label ~ features")
    println(RFormula)
    val formula = new RFormula()
      .setFormula(RFormula)
      .setFeaturesCol("features")
      .setLabelCol("label")
    val outputData = formula.fit(data).transform(data) 

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main1: Prepared Features and Label for the Tree")
       
    return outputData
  }  
  
 
  
  //////////////////////////////////////////////////////////////////////////
  // Function to make prediction for every business segment and accumulate the data
  def classificationTreesByBusinessSegments(inData: DataFrame) 
  :DataFrame = 
  {
    
    // Get the list of business segments
    var data = inData
    data.persist()
    data.registerTempTable("data")

//    val SegmentList = sqlContext.sql("select distinct " + busSegmentName + " from data").rdd.map(r => r(0)).collect()
    var SegmentList = sqlContext.sql("select " + busSegmentName + " from data group by " + busSegmentName + " order by count(1) desc").rdd.map(r => r(0)).collect()
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO classificationTreesByBusinessSegments: Business segments to loop over:")
    println(SegmentList.mkString(", "))
    
    // Prepare dataframe for accumulation
    var predictionLeaf:DataFrame = null
    
    // Loop over business segments
    for (aSegment <- SegmentList)
    {
      
      // Log
      println("")
      println("**********************************************")
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO classificationTreesByBusinessSegments: Process business segment: " + aSegment.asInstanceOf[String] )
   
      // Filter out a business segment data
      val data1BSegment = data.filter(busSegmentName + " = '" + aSegment + "'")
      println("Count of records in the segment:")
      println("data1BSegment.count() = " + data1BSegment.count())
          
      // Categorize the target variables
      val dataCatTarget = categorizeTarget(data1BSegment)
      
      // Run a decision tree
      val prediction1BSegment = classificationTreePredict(dataCatTarget, aSegment.asInstanceOf[String])
      
      // Adapt the id of the micro-segments to the Business segments 
      val prediction1BSAdapted = prediction1BSegment
        .withColumn("BLeafID", concat(lit(aSegment.asInstanceOf[String].trim()), lit("_"), prediction1BSegment("LeafID") ))  
        .drop("LeafID")

      // Log
      println("Data with BLeafIDs:")
      prediction1BSAdapted.show()
      println("Distinct BLeafIDs:")
      prediction1BSAdapted.registerTempTable("prediction1BSAdapted")
      sqlContext.sql("select distinct BLeafID from prediction1BSAdapted").show()
      
      val forecasdBLeafEssential = sqlContext.sql("select " 
          + keyColumns.mkString(", ") + ", " 
          + busSegmentName + ", " 
          + targetColNames.mkString(", ") 
          + ", BLeafID from prediction1BSAdapted")
    
      // Accumulate the segmentation
      if (predictionLeaf == null)
      {
        predictionLeaf = forecasdBLeafEssential
      }
      else
      {
        println("Accumulating foreecast for business segment")
        predictionLeaf.printSchema()
        prediction1BSAdapted.printSchema()
        predictionLeaf = predictionLeaf.unionAll(forecasdBLeafEssential)
      }
      
    }   
    
   
    return(predictionLeaf)
  }
 
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function Calibrate the classification tree model and make prediction
  def classificationTreePredict(data: DataFrame, busSegmName: String = null) 
  :DataFrame = 
  {
    // Path
    val loadModelPath = SegmentationModelPath + "_" + busSegmName.replaceAll(" ", "_")
    
    // Load three model
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO classificationTree: Loading the model from a file " + loadModelPath)
    val FeaturesModelLoaded = sc.objectFile[(Array[String], PipelineModel)](loadModelPath).first()
    val columnsListLoaded = FeaturesModelLoaded._1
    val ModelLoaded = FeaturesModelLoaded._2
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO classificationTree: Loaded the model from a file " + loadModelPath)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO classificationTree: Use the following columns: ")
    println(columnsListLoaded.mkString(", "))
    
    // Preselect the columns that belong to that model
    data.registerTempTable("data")
    val dataSelect = sqlContext.sql("select " + columnsListLoaded.mkString(", ") + " from data")
    
    // Prepare features and label columns
    val dataFeatLab = prepareTreeFeaturesLabel(dataSelect)
    
    // Make prediction
    val predictions = ModelLoaded.transform(dataFeatLab)
    predictions.coalesce(36)
    
    // Log
    val treeModel = ModelLoaded.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    predictions.show(5)

    // Log: Select distinct predicted values
    predictions.registerTempTable("predictions")
    val distPredictions = sqlContext.sql("select count(distinct rawPrediction) from predictions").first().getLong(0)
    println("The number of distinct leafs = number of microsegments = " + distPredictions)
    
    // Convert rawPrediction to a simpler id
    val predictionsStr = sqlContext.sql("select *, cast(rawPrediction as string) as rawPredictionStr from predictions")
    val leafIndexer = new StringIndexer()
      .setInputCol("rawPredictionStr")
      .setOutputCol("LeafID")
      .fit(predictionsStr)
    val predictionLeaf = leafIndexer.transform(predictionsStr)

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO classificationTree: Calibrated a classification tree")
    
    return predictionLeaf
  }
  
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to count values and their standard deviations per segment and save the data
  def calculateSegmentAggregates(predictionLeaf: DataFrame) 
  {
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentAggregates: started")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentAggregates: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")

    
    // Extract segment facts from prediction
    predictionLeaf.registerTempTable("predictionLeaf")
    var SegmentationDF = sqlContext.sql("select '"+ actualDate + "' as ActualMonth, " + keyColumns.mkString(", ") + ", BLeafID as MicroSegmentID, 1 as Probability from predictionLeaf")
    SegmentationDF.cache()
    println("SegmentationDF table: ")
    SegmentationDF.show(5)
    println("SegmentationDF.count() = " + SegmentationDF.count())
 
    
    // Save Segment facts
    if (saveFlag) {
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentAggregates: Saving SegmentationDF")
      val fs:FileSystem = FileSystem.get(new URI(SegmentationOutPath), sc.hadoopConfiguration);
      fs.delete(new Path(SegmentationOutPath), true) // true for recursive
      SegmentationDF.coalesce(12).save(SegmentationOutPath)           // parquet
      FileUtil.chmod(SegmentationOutPath, "999", true)
    }
    
    SegmentationDF.persist()
      
//    // Export Segmentation facts as CSV
//    val segmentationFactsCSVFormatted = SegmentationDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
//    segmentationFactsCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/SegmentFactsCSV")   // 
    
      
    // Extract segment properties
    val SegmentsData = sqlContext.sql("select '"+ actualDate + "' as ActualMonth, BLeafID as MicroSegmentID, " +
        busSegmentName  + ", " +
        "avg(" + targetColNames(0) + ") as RevenueFrc, avg(" + targetColNames(1) + ") as ChurnScoreFrc, " +
        "stddev(" + targetColNames(0) + ") as RevenueFrcStd, stddev(" + targetColNames(1) + ") as ChurnScoreFrcStd, 'BlaBla' as SegmentDescription " +
        "from predictionLeaf " + 
        "group by BLeafID, " + busSegmentName + ", '"+ actualDate + "' " + 
        "order by MicroSegmentID ")
    SegmentsData.show()
    
    
    // Save Segment properties
    if (saveFlag) {
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentAggregates: Saving SegmentsData")
    val fs1:FileSystem = FileSystem.get(new URI(SegmentDataOutPath), sc.hadoopConfiguration);
    fs1.delete(new Path(SegmentDataOutPath), true) // true for recursive
    SegmentsData.coalesce(12).save(SegmentDataOutPath)        // parquet
    FileUtil.chmod(SegmentDataOutPath, "999", true)
    }
    
    
//    // Export D_SEGMENTS locally into CSV
//    val SegmentsDataCSVFormatted = SegmentsData.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
//    SegmentsDataCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/SegmentDataCSV")     // locally   

  }
    
  
    
  //////////////////////////////////////////////////////////////////////////
  // Function to assign categories to target 
  def categorizeTarget(inData: DataFrame) 
  :DataFrame = 
  {

    // Set the number of bins for each of target variables.
    // NB: Affects performance. Use as few as possible
    val countBuckets0 = 15
    val countBuckets1 = 5
    println(countBuckets0, countBuckets1)
    
    val inDataCount = inData.count() 
    val fraction = Math.min(5000.0, inDataCount)/ inDataCount
    
    // Discretise each of the target variables
    val discretiser0 = new QuantileDiscretizer()
      .setInputCol(targetColNames(0))
      .setOutputCol("TargetCat0")
      .setNumBuckets(countBuckets0)
    val bucketiser0 = discretiser0.fit(inData.sample(false, fraction))
    var outData = bucketiser0
      .transform(inData)
      
    val discretiser1 = new QuantileDiscretizer()
      .setInputCol(targetColNames(1))
      .setOutputCol("TargetCat1")
      .setNumBuckets(countBuckets1)
    val bucketiser1 = discretiser1.fit(outData.sample(false, fraction))
    outData = bucketiser1
      .transform(outData)
      
    // Make one TargetCat from all of them 
    outData = outData.withColumn("TargetCatDouble", outData("TargetCat0") 
        + outData("TargetCat1") * countBuckets0)
    outData = outData.withColumn(targetColumn, outData("TargetCatDouble").cast("Int"))
           
    // Log results
    outData.registerTempTable("outData")
    val logTab = sqlContext.sql("select " + targetColNames.mkString(", ") 
        + ", TargetCat0, TargetCat1, " + targetColumn + " from outData")
    println("Dataset with categorised target:")
    logTab.show(5)
    
    // Clean the dataFrame
    outData = outData.drop("TargetCat0").drop("TargetCat1").drop("TargetCat2").drop("TargetCatDouble")
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO main1: Categorized Target variables")
    
    return outData
  }
  
}


    