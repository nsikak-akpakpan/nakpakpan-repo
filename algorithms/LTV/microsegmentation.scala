/* 
BEHAVIORAL MICROSEGMENTATION OF CLIENT*MONTH HISTORICAL DATA
CALIBRATES THE MICROSEGMENTATION MODEL AND SAVES IT TO DISK


LAST RUN TIME
Cluster: 8h
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
object main0 
{
  // Configuration
  val targetColNames = Array("MonthlyRevenue", "ChurnScore") 
  val targetColumn = "TargetCat"
  val keyColumns = Array("ClientID", "Month") 
  val busSegmentName = "BusSegment"
  val isNewJoinerColName = "IsNewJoiner"
  val isChurnColName = "IsChurn"
  
  var sqlContext: org.apache.spark.sql.SQLContext = null
  var sc: org.apache.spark.SparkContext = null;
  
//  val CLVInputAbsPath = "hdfs://10.82.187.10:8020/data/modeling/churn/INPUT_CLV"
//  val SegmentationModelAbsPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/M_SEGMENTATION_MODEL"
//  
//  val CLVInputRelPath = "/data/modeling/churn/INPUT_CLV"
//  val SegmentationModelRelPath = "/data/modeling/CLV/M_SEGMENTATION_MODEL"
  
  var CLVInputPath:String = null
  var SegmentationModelPath:String = null

  var actualDate: String = null
  var startTime: Date = null
  var dateTimeFormatter: DateFormat = null
  var inputDataLength: Int = 0
  
  val submissionFlag = true // !! Option for submission. Make true when spark-submitting !!
  val correlThreshold = 0.7
  val numColumnsUse = 20
  val saveGlobal = true  // Option for output
  val seed = 1
  

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
        
    // Fit and save the Classification tree model
    val dataWSegments = classificationTreesByBusinessSegments(dataWithoutSpSegments)
    
     // Log
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO main: finished")
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO main: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")
       
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
        .cache()
        
    val dataWithSpSegments = sqlContext.sql("select * from dataAll " +
        "where " + isChurnColName + " = true " + 
        "or " + isNewJoinerColName + " = true ")
    
    // Update BusSegment by corresponding leave segment name
    dataWithSpSegments.registerTempTable("dataWithSpSegments")
    val dataWithSpSegmentsBSUpdated = sqlContext.sql("select *, case " + 
        "when " + isChurnColName + "= true then '" + isChurnColName + "' " +
        "when " + isNewJoinerColName + "= true then '" + isNewJoinerColName + "' " +
        "else " + busSegmentName + " " + 
        "end as BusSegmentUpdated from dataWithSpSegments")
        .drop(busSegmentName)
        .withColumnRenamed("BusSegmentUpdated", busSegmentName)
        .cache()
        
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
    conf.set("spark.executor.memory", "6154m") 
    conf.set("spark.driver.memory", "3g") 
    conf.set("spark.executor.cores", "8") 
    conf.set("spark.cores.max", "infinite")
    conf.set("spark.yarn.driver.memoryOverhead", "1024")
    conf.set("spark.sql.shuffle.partitions", "36")
    conf.set("spark.driver.maxResultSize", "16000m")
    
    // Rewrite the file if exists
    conf.set("spark.hadoop.validateOutputSpecs", "false") 
    
    // Contexts
    sc = new SparkContext(conf);
    sqlContext = new org.apache.spark.sql.SQLContext(sc)  
    
    CLVInputPath = args(0) 
    SegmentationModelPath = args(1) 
     
  }
 
  
  
  //////////////////////////////////////////////////////////////////////////
  // Get data function
  def getCLVData() 
  :DataFrame = 
  {

  val inputDF = sqlContext.read.load(CLVInputPath).coalesce(12).cache()  
    
    // Take only last 6 months of the data
    inputDF.registerTempTable("inputDF")
    val dataDF_CLV = sqlContext.sql("select * from inputDF where Month >= '2014-01-30'")
    
    // For training the model sample the data
//    val DataCount = dataDF_CLV.count()
//    val fraction = Math.min(1000000.0, DataCount) / DataCount
//    println("fraction = " + fraction + ", DataCount = " + DataCount)
    
    // TEMP REMOVE
    dataDF_CLV.registerTempTable("dataDF_CLV")
   //  val dataDF_CLV_sample = sqlContext.sql("select ClientID, Month, MonthlyRevenue, BusSegment, IsNewJoiner, IsChurn, IsWriteOff, ChurnScore, DelinquentBucket, RiskSegment from dataDF_CLV")
      //.sample(false, fraction)
    val dataDF_CLV_sample = dataDF_CLV
      .drop("RiskSegment")
      .drop("NumberOfSavingsAccounts")
      .drop("ActiveToAllCreditsRatio")
    
    dataDF_CLV_sample.coalesce(36).persist()  
    println("Read in dataDF_CLV_sample with row count = "  + dataDF_CLV_sample.count())
    return dataDF_CLV_sample
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
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO treatNullValues: Started ")
    
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
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO treatNullValues: Finished")
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO treatNullValuess: Minimal/maximal ChurnScore = ")
    sqlContext.sql("select min(ChurnScore), max(ChurnScore) from dataNumericNotNull ").show()
    
    //Return
    return dataNumericNotNullNull
  }
  
  

  //////////////////////////////////////////////////////////////////////////
  // Function to filter out string columns that have more than 32 distinct values
  def filterFalseStrings(inData: DataFrame)
  : DataFrame = 
  {
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO filterFalseStrings: started")

    // Get list of types
    val types = inData.dtypes
      .filterNot(p => targetColNames.contains(p._1))
      .filterNot(p => keyColumns.contains(p._1))
      .filterNot(p => Array(targetColumn).contains(p._1))
      .filterNot(p => Array(busSegmentName).contains(p._1))
      .filterNot(p => Array(isNewJoinerColName).contains(p._1))
      .filterNot(p => Array(isChurnColName).contains(p._1))
    
    var outData = inData.persist()
    
    // Loop over columns
    for (colNameType <- types){
      // println("colNameType._1.substring(colNameType._1.length()-10) = " + colNameType._1.substring(Math.max(colNameType._1.length()-10,0)))
      if (colNameType._2 == "StringType"
          || colNameType._1.substring(Math.max(colNameType._1.length()-10,0)) == "StringLong") {
        outData.registerTempTable("outData")
        val distCount = sqlContext.sql("select count(distinct " + colNameType._1 + ") from outData").first().getLong(0)
        println("Variable " + colNameType._1 + " has " + distCount + " distinct values") 
        if (distCount > 31) {
          println("Column " + colNameType._1 + " is string, has " + distCount + " distinct values and is FILTERED OUT")
          outData = outData
            .drop(colNameType._1)
        }
         
      }
    }
    
    outData.unpersist()
    inData.unpersist()
    
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO filterFalseStrings: finished") 
    
    return (outData)
  }
  
  
 
  //////////////////////////////////////////////////////////////////////////
  // Function to filter columns from the dataset by different statistical methods 
  def choseModel(inData: DataFrame)
  : DataFrame = 
  {
 
    // Remove columns with one distinct value
    val dataDistinct = filterOneDistinct(inData) 
    
    // Remove string columns with more than 32 distinct values
    val dataNoFalseStrings = filterFalseStrings(dataDistinct)
 
    // Remove correlated variable
    val dataWOCorrelated = removeCorrelated(dataNoFalseStrings)
    
    // Estimate significance of the variables by fitting a tree having this variable the only predictor
    val outData = leaveSignificant(dataWOCorrelated)
    
    return outData
  }
    
    
    
  //////////////////////////////////////////////////////////////////////////
  // Function to calculate a statistics of the forecast
  def calculateSegmentationStatistics(inData: DataFrame)
  : Double = 
  {
    val inDataCopy = inData
    inDataCopy.registerTempTable("inDataCopy")
    
    inDataCopy.printSchema()
    inDataCopy.show(5)
    
    // Calculate rate of correct guesses
    val GuessData = sqlContext.sql("select * from inDataCopy where label = predictedLabel")
    val rateGuess = GuessData.count().toDouble / inData.count().toDouble
    
    // Log
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO calculateSegmentationStatistics: Guess Rate = " + rateGuess)
    
    return rateGuess
  }
    
    
    
  //////////////////////////////////////////////////////////////////////////
  // Function to leave only columns that make forecasts with highest forecast rate
  def leaveSignificant(inData: DataFrame)
  : DataFrame = 
  {
    // Log
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO leaveSignificant: Started ")
    println("inData.count() = " + inData.count())
    
    // Get the list of effects
    val effectNames = inData.columns
      .filterNot( targetColNames.toSet)
      .filterNot(keyColumns.toSet)
      .filterNot(Array(targetColumn).toSet)
      .filterNot(Array(busSegmentName).toSet)
      .filterNot(Array(isNewJoinerColName).toSet)
      .filterNot(Array(isChurnColName).toSet)
    println("List of effects to go through:")
    println(effectNames.mkString(", "))
    
    val copyInData = inData.persist()
    copyInData.registerTempTable("copyInData")
    
    var varsSignifs: Array[(String, Double, Long)] = null
    
    // Sample the data to train
    val DataCount = copyInData.count()
    val fraction = Math.min(100000.0, DataCount) / DataCount
    println("fraction = " + fraction + ", DataCount = " + DataCount)
    val trainInData = copyInData
      .sample(false, fraction)
    trainInData.persist()

    // Print statistics on the input set
    trainInData.registerTempTable("trainInData")
    println("trainInData.rdd.partitions.length = " + trainInData.rdd.partitions.length)
    
    // Loop over effects
    var counter:Int = 1
    val totalCount = effectNames.length
    
    for (effectName <- effectNames){
      
      // Log
      println("")
      println("************ Looping over effects")
      println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO leaveSignificant: Trying " + effectName)
      println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO leaveSignificant: Looping over columns to filter. Progress " + counter + "/" + totalCount)
      counter = counter + 1

      // Separate data with one effect only
      val data1EffectArray = sqlContext.sql("select " + effectName  + ", " + targetColumn +
          " from trainInData")
      println("Schema before collect():")
      data1EffectArray.printSchema()
     
      // Collect to avoid lazy evaluation
      val data1Effect = sqlContext.createDataFrame(sc.makeRDD(data1EffectArray.collect()), data1EffectArray.schema);
      println("Schema after collect():")
      data1Effect.printSchema()
      
      // Run a decision tree, make forecast
      val prediction = classificationTree(data1Effect, true)
      
      // Calculate the variation coefficient
      val statCoef = calculateSegmentationStatistics(prediction)
      
      // Calculate count of leaves
      prediction.registerTempTable("prediction")
      val countLeafs = sqlContext.sql("select count(distinct LeafID) from prediction").first().getLong(0)
      
      // Accumulate statistics
      if (varsSignifs == null){
        varsSignifs = Array((effectName, statCoef, countLeafs))
      }else{
        varsSignifs = varsSignifs :+ (effectName, statCoef, countLeafs)
      }
      
    }
    
    copyInData.unpersist()
    inData.unpersist()
    trainInData.unpersist()
    
    // Log
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO leaveSignificant: significance of all variables: ")
    println(varsSignifs.map(p => p._1 + " " + p._2 + " " + p._3).mkString(", "))
    
    // Drop columns with the least possible variation coefficient and number of branches
    val minScore = varsSignifs.map(p => p._2).min
    val minLeavesCount = varsSignifs.map(p => p._3).min
    var varsSignifsAllNotLeast = varsSignifs.filter(P => (P._2 > minScore) )
    if (varsSignifsAllNotLeast.length == 0){
      varsSignifsAllNotLeast = varsSignifs
    }
    println("minScore = " + minScore + ", minLeavesCount = " + minLeavesCount)
    
    // Select top 20 columns basing on variation coefficient and number of leaves
    val sorted = varsSignifsAllNotLeast.sortBy(r => (r._2, -r._3))
    val topValues = sorted.drop(Math.max(0, sorted.length - numColumnsUse))
    val colNamesSelected = topValues.map(p => p._1) 
    println("Out of them left in the model:")
    println(topValues.map(p => p._1 + " " + p._2 + " " + p._3).reverse.mkString(", "))
    
    // Select data by the remaining columns
    inData.registerTempTable("inData")
    val outData = sqlContext.sql(" select " + 
        keyColumns.mkString(", ") + ", " + 
        targetColNames.mkString(", ") + ", " +
        targetColumn + ", " +
        busSegmentName + ", " +
        colNamesSelected.mkString(", ") +
        " from inData")
    
    println("outData.count() = " + outData.count())    
        
    return outData
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
       
    
    // Index the strings
    var outData = indexAllStrings(dataNumericNotNull)
    outData.persist()
   
    
    //Log
    println("\n" + dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO clearData: outData.count() = " + outData.count())
    println("\n" + dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO clearData: filtered data")
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO clearData: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")
    
    return outData 
  }

    
  
  //////////////////////////////////////////////////////////////////////////
  // Function to index all string columns
  def indexAllStrings(inData: DataFrame)
  : DataFrame = 
  {
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO indexAllStrings: begin ")
    
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

    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO indexAllStrings: end")
    
    // Return
    return outData
  }
 
  
  
  //////////////////////////////////////////////////////////////////////////
  // Convert an MLlib matrix to a Breeze dense matrix 
  def mllibMatrixToDenseBreeze(matrix: org.apache.spark.mllib.linalg.Matrix): breeze.linalg.DenseMatrix[Double] = {
    matrix match {
      case dense: org.apache.spark.mllib.linalg.DenseMatrix => {
        if (!dense.isTransposed) {
          new breeze.linalg.DenseMatrix[Double](dense.numRows, dense.numCols, dense.values)
        } else {
          val breezeMatrix = new breeze.linalg.DenseMatrix[Double](dense.numRows, dense.numCols, dense.values)
          breezeMatrix.t
        }
      }

      case _ => new breeze.linalg.DenseMatrix[Double](matrix.numRows, matrix.numCols, matrix.toArray)
    }
  }
  
  
  
  //////////////////////////////////////////////////////////////////////////
  //  Conversion of Boolean to Int
  implicit def bool2int(b:Boolean) = if (b) 1 else 0
  
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to remove columns that are too correlated
  def removeCorrelated(inData: DataFrame)
  : DataFrame = 
  {
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO removeCorrelated: Started ")
    println("inData.count() = " + inData.count())
    
    // Make a copy to transform
    var inDataCpy = inData
    inDataCpy.persist()
   
    // Sample the data
    val DataCount = inDataCpy.count()
    val fraction = Math.min(100000.0, DataCount) / DataCount 
    val inDataSample = inDataCpy
      .sample(false, fraction)
      
    println("After sampling inData.count() = " + inDataSample.count())
    
    // Assemble into vectors
    val colNamesNum = inDataSample.columns
      .filterNot( targetColNames.toSet)
      .filterNot(keyColumns.toSet)
      .filterNot(Array(targetColumn).toSet)
      .filterNot(Array(busSegmentName).toSet)
      .filterNot(Array(isNewJoinerColName).toSet)
      .filterNot(Array(isChurnColName).toSet)
    val assembler = new VectorAssembler()
      .setInputCols(colNamesNum)
      .setOutputCol("features")
    val vectorData = assembler.transform(inDataSample)
    
    // Transform to RDD
    val RddVectorData = vectorData
      .select("features")
      .rdd
      .map(_.getAs[org.apache.spark.mllib.linalg.Vector]("features"))
    RddVectorData.persist()
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO removeCorrelated: Prepared data for correlation matrix ")

    // Save the RDD as CSV
    // rddVectorData.saveAsTextFile("C:/Work/Projects/C360/Data/MarkovChain/RDDVectorCorr.csv")

      
    // Compute correlation matrix
    val correlMatrix: org.apache.spark.mllib.linalg.Matrix = Statistics.corr(RddVectorData, "spearman") // "pearson" another option
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO removeCorrelated: Computed correlation matrix ")
    RddVectorData.unpersist()
    
//    // Save the matrix as CSV
//    val localMatrix: List[Array[Double]] = correlMatrix
//      .transpose  // Transpose since .toArray is column major
//      .toArray
//      .grouped(correlMatrix.numCols)
//      .toList
//
//    val lines: List[String] = localMatrix
//      .map(line => line.mkString(" "))
//
//    sc.parallelize(lines)
//      .repartition(1)
//      .saveAsTextFile("C:/Work/Projects/C360/Data/MarkovChain/correlMatrix.txt")

    
    // Convert to a mutable Breeze Matrix
    var correlMatrixCopy = mllibMatrixToDenseBreeze(correlMatrix)  // Make a copy that will be modified in a loop

    // Set diagonal elements to zero
    val identity = breeze.linalg.DenseMatrix.eye[Double](correlMatrixCopy.cols)
    correlMatrixCopy = correlMatrixCopy - identity  
    
    // Loop over rows and remove correlated
    for (row <- (0 to (correlMatrix.numRows-1))){
      var isCorr = 0
      var corCoeff = 0.0
      for (col <- (0 to (correlMatrix.numCols-1))){
        corCoeff = Math.max(corCoeff, correlMatrixCopy.apply(row, col))
        isCorr = isCorr + bool2int(Math.abs(corCoeff) > correlThreshold)
      }
      if (isCorr > 0) {
        // Log
        println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO removeCorrelated: removing " + colNamesNum(row) + " with correlation with another variable " + corCoeff)
        
        // Remove the variable from dataFrame
        inDataCpy = inDataCpy.drop(colNamesNum(row))
        
        // Set the corresponding column and row in correlation matrix to zero not to account twice the same variable
        for (idxUpdate <- (0 to (correlMatrix.numCols-1))){
          correlMatrixCopy(row, idxUpdate) = 0
        }
        for (idxUpdate <- (0 to (correlMatrix.numRows-1))){
          correlMatrixCopy(idxUpdate, row) = 0
        }
      }
    }

    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO removeCorrelated: After removal remains: ")
    inDataCpy.printSchema()
    inDataCpy.unpersist()
    
    val outData = inDataCpy
    
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO removeCorrelated: Finished ")
    println("outData.count() = " + outData.count())
    
    return (outData)
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to filter columns from the dataset that have only one distinct values
  def filterOneDistinct(inData: DataFrame)
  : DataFrame = 
  {
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO filterOneDistinct: Started")
   
    var dataDistinct = inData.persist()
    
    // Get the list of features column names 
    val colNames = dataDistinct.columns
      .filterNot( targetColNames.toSet)
      .filterNot(keyColumns.toSet)
      .filterNot(Array(targetColumn).toSet)
      .filterNot(Array(busSegmentName).toSet)
      .filterNot(Array(isNewJoinerColName).toSet)
      .filterNot(Array(isChurnColName).toSet)
    println("Columns to proceed: ")
    println(colNames.mkString(", "))
     
    // Determine columns with one distinct value
    // Compare min<>max
    dataDistinct.registerTempTable("dataDistinct") 
    var distColNames: Array[String] = null
    for (colName <- colNames) 
    {
      // println("Processing " + colName)
      val minIsMax = sqlContext.sql("select min(" + colName + ") = max(" + colName + ") as minIsMax from dataDistinct").first().getBoolean(0) // MINMAX
      if (minIsMax)
      {
        dataDistinct = dataDistinct.drop(colName)
        println("Column " + colName + " has 1 distinct value and is FILTERED OUT")
      }
    }
    
    inData.unpersist()
    dataDistinct.unpersist()
    
    // Log    
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO filterOneDistinct: finished")
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO filterOneDistinct: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime())/1000.0 + " seconds")

    return dataDistinct 
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
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO main1: Prepared Features and Label for the Tree")
       
    return outputData
  }  
  
 
  
  //////////////////////////////////////////////////////////////////////////
  // Function to make prediction for every business segment and accumulate the data
  def classificationTreesByBusinessSegments(inData: DataFrame) 
  {
    
    // Get the list of business segments
    var data = inData
    data.persist()
    data.registerTempTable("data")

//    var SegmentList = sqlContext.sql("select distinct " + busSegmentName + " from data").rdd.map(r => r(0)).collect()
    var SegmentList = sqlContext.sql("select " + busSegmentName + " from data group by " + busSegmentName + " order by count(1) desc").rdd.map(r => r(0)).collect()
    
    // Log
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO classificationTreesByBusinessSegments: Business segments to loop over:")
    println(SegmentList.mkString(", "))
    
    // Prepare dataframe for accumulation
    var predictionLeaf:DataFrame = null
    
    // DEBUG
    // SegmentList = Array("Mass market")
    
    // Loop over business segments
    for (aSegment <- SegmentList)
    {
      
      // Log
      println("")
      println("**********************************************")
      println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO classificationTreesByBusinessSegments: Process business segment: " + aSegment.asInstanceOf[String] )
   
      // Filter out a business segment data
      val data1BSegment = data.filter(busSegmentName + " = '" + aSegment + "'")
      println("Count of records in the segment:")
      println("data1BSegment.count() = " + data1BSegment.count())
          
      // Categorize the target variables
      val dataCatTarget = categorizeTarget(data1BSegment)
      
       // Choose the model = filter out some of the columns
      val dataChosen = choseModel(dataCatTarget)
      
      // Run a decision tree
      val prediction1BSegment = classificationTree(dataChosen, false, aSegment.asInstanceOf[String])
      
    }   
   
  }
 
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function Calibrate the classification tree model and make prediction
  def classificationTree(data: DataFrame, modeSelectionFlag: Boolean, busSegmName: String = null) 
  :DataFrame = 
  {
    // Define the list of columns left for the prediction
    val columnList = data.columns
    
    // Prepare features and label columns
    val dataFeatLab = prepareTreeFeaturesLabel(data)
    
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(dataFeatLab)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
      .fit(dataFeatLab)
      
    // Train a DecisionTree model.
    val dtBasic = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxDepth(4)
      .setMinInstancesPerNode(1000)
    //  .setSeed(seed)
    // TODO: The minimal node value should be defined by sample volume rather than hardcoded
      
    // Basing on submission flag setMaxMemoryInMB
    var dt:DecisionTreeClassifier = null 
    if(submissionFlag){
      dt = dtBasic
        .setMaxMemoryInMB(80000) 
        .setCacheNodeIds(true)
        .setCheckpointInterval(10)          
    }else{
      dt = dtBasic
        .setMaxMemoryInMB(4096)
    }
    
     
    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    
    // Chain indexers and tree in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
    
    // Train model.  This also runs the indexers.
    val model = pipeline.fit(dataFeatLab)
    
    
    
    // // Save the model
    if (!modeSelectionFlag){
      if (saveGlobal){
        // Path
        val savePath = SegmentationModelPath + "_" + busSegmName.replaceAll(" ", "_")
        
        // Save
        println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO classificationTree: Saving the model to a file " + savePath)
        val FeaturesModel = (columnList, model)
        sc.parallelize(Seq(FeaturesModel), 1).saveAsObjectFile(savePath)
      }
    }
      
    
  
    // Make prediction
    val predictions = model.transform(dataFeatLab)
    
    // Log
    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    predictions.show(5)

    // Log: Select distinct predicted values
    predictions.registerTempTable("predictions")
//    val distPredictions = sqlContext.sql("select count(distinct rawPrediction) from predictions").first().getLong(0)
//    println("The number of distinct leafs = number of microsegments = " + distPredictions)
    
    // Convert rawPrediction to a simpler id
    val predictionsStr = sqlContext.sql("select *, cast(rawPrediction as string) as rawPredictionStr from predictions")
    val leafIndexer = new StringIndexer()
      .setInputCol("rawPredictionStr")
      .setOutputCol("LeafID")
      .fit(predictionsStr)
    val predictionLeaf = leafIndexer.transform(predictionsStr)

    // Log
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO classificationTree: Calibrated a classification tree")
    
    return predictionLeaf
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
    
    // WARNING: The discretiser fails on bigger data
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
    println(dateTimeFormatter.format(Calendar.getInstance().getTime()) + " INFO main1: Categorized Target variables")
    
    return outData
  }
  
}


    