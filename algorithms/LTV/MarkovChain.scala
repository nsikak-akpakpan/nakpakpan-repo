// Calculates predictions basing on Markov Chain model
// Uses data from microsegmentation




package MarkovChain

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType };
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.time.LocalDate
import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI
import org.joda.time.{ DateTime, Period }
import org.joda.time.format.DateTimeFormat

import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix, SingularValueDecomposition, DenseMatrix, DenseVector }
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry }
import org.apache.spark.ml.feature.{ StringIndexer, IndexToString, VectorIndexer }

object main2 {

  
  
  //////////////////////////////////////////////////////////////////////////
  // Cofiguration
  var sqlContext: org.apache.spark.sql.SQLContext = null
  var sc: org.apache.spark.SparkContext = null;

  val NewJoinerColName = "IsNewJoiner"
  val ChurnColName = "IsChurn"
 
//  val SegmentDataInAbsProdPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/D_SEGMENTS"
//  val SegmentCLVDataOutAbsProdPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/D_SEGMENTS_CLV"
//  val CustomerSegmentFactInAbsProdPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/F_SEGMENTS"
//  val TransitionMatrixOutAbsProdPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/TRANSITION_TABLE"
//  val segmentFutureFactDataAbsProdPath = "hdfs://10.82.187.10:8020/data/modeling/CLV/F_SEGMENTS_FUT_PAST"
//
//  val SegmentDataInRelProdPath = "/data/modeling/CLV/D_SEGMENTS"
//  val SegmentCLVDataOutRelProdPath = "/data/modeling/CLV/D_SEGMENTS_CLV"
//  val SegmentationInRelProdPath = "/data/modeling/CLV/F_SEGMENTS"
//  val TransitionMatrixOutRelProdPath = "/data/modeling/CLV/TRANSITION_TABLE"
//  val segmentFutureFactDataRelProdPath = "/data/modeling/CLV/F_SEGMENTS_FUT_PAST"
  
  var SegmentDataInPath: String = null
  var SegmentCLVDataOutPath: String = null
  var CustomerSegmentFactInPath: String = null
  var TransitionMatrixOutPath: String = null
  var segmentFutureFactDataPath: String = null

  var actualMonthDate: String = null
  var latestTimeInFacts: DateTime = null
  var startTime: Date = null

  val discountFactor = 0.0016515813 // Future value in ONE PERIOD! is calculated by multiplying amount by (1 + discountFactor). Corresponds to 2% annually
  val forecastHorizonTotal: Int = 6
  val submissionFlag = true // Option for submission. Make true when spark-submitting
  val saveFlag = true

  
  
  //////////////////////////////////////////////////////////////////////////
  // Main function
  def main(args: Array[String]) {

    // Log
    startTime = Calendar.getInstance().getTime()
    println(startTime.toLocaleString() + " INFO MarkovChain: Started")

    // Initialize
    Initialise(args)

    // Get the fact data
    var segmentFactData = getSegmentFactData()

    // Calculate transition matrix
    val transitionMatrix = calculateTransitionMatrix(segmentFactData) 

    // Get D_SEGMENTS data
    val segmentDData = getSegmentsDimensionData()

    // Calculate CLV for segments
    val segmentsCLV = calculateSegmentCLV(transitionMatrix, segmentDData)

    // Forecast probabilities of each client residing in a segment in future months
    val segmentFactFutureData = generateCustomersSegmentsFutPast(transitionMatrix, segmentFactData)

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO MarkovChain: finished")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO MarkovChain: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to compute inverse of a RowMatrix
  def computeInverse(X: IndexedRowMatrix)
  : DenseMatrix = 
  {
    // DEBUG LINE X.rows.toArray()
    val nCoef = X.numCols.toInt
    val svd = X.computeSVD(nCoef, computeU = true)
    if (svd.s.size < nCoef) {
      sys.error(s"RowMatrix.computeInverse called on singular matrix.")
    }

    // Create the inv diagonal matrix from S 
    val invS = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x, -1))))

    // U cannot be a RowMatrix
    val U = svd.U.toBlockMatrix().toLocalMatrix().multiply(DenseMatrix.eye(svd.U.numRows().toInt)).transpose
 
    // If you could make V distributed, then this may be better. However its alreadly local...so maybe this is fine.
    val V = svd.V
    // inv(X) = V*inv(S)*transpose(U)  --- the U is already transposed.
    (V.multiply(invS)).multiply(U)
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to count average number of new joiners per month
  def countMonthlyNewJoiners(factsTable: DataFrame)
  : Int =
  {
    factsTable.cache()
    factsTable.registerTempTable("factsTable")

    // Get the total count of new joiners in fact data
    println("distinct MicroSegmentID: ")
    sqlContext.sql("select distinct MicroSegmentID from factsTable").show(100)
    val totalNJCount = sqlContext.sql("select count(1) from factsTable where MicroSegmentID = '" + NewJoinerColName + "'").first().getLong(0)

    // CLV MIGRATION
    // Get the count of months between first and last records
    val countMonths = sqlContext.sql("select round(months_between(max(Month), min(Month))) + 1 as monthCount from factsTable").first().getDouble(0)
    sqlContext.sql("select max(Month), min(Month) from factsTable").show()

    // Calculate the average number of New Joiners per month
    val AvgMnthlyCnt = (totalNJCount.toFloat / countMonths.toFloat).floor.toInt

    // Log
    println("countMonths = " + countMonths)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO countMonthlyNewJoiners: Average count of New Joiners every month " + AvgMnthlyCnt)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO countMonthlyNewJoiners: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    return (AvgMnthlyCnt)
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to calculate future probabilities of customers to be in segments
  def forecastClientMSProb(transitionMatrix: DataFrame, segmentFactData: DataFrame)
  : DataFrame =
  {
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientMSProb: Started ")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientMSProb: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    // TODO: Control that all active clients have last month segment. Otherwise problems predicting from the last month.
    // Get the clients and their segments corresponding to the latestTime
    val currentClientSegment = sqlContext.sql("select * from segmentFactData where Month = '" + latestTimeInFacts.toLocalDate().toString() + "'")

    // LOG DEBUG
    // currentClientSegment.filter("MicroSegmentID = 'Churn'").sort("ClientID").show()
    //    currentClientSegment.sort("ClientID").show()
    //    transitionMatrix.show()

    // NB: Ignore the repeated churned clients from future. As already done in data preparation.
    // Get the segment probabilities for clients
    currentClientSegment.registerTempTable("currentClientSegment")
    val segmentFutureData = sqlContext.sql("select " +
      "cCS.ActualMonth, cCS.ClientID, add_months(cCS.Month, tM.Month) as Month, tM.MicroSegmentIDTo as MicroSegmentID, tM.Probability " +
      "from currentClientSegment cCS join transitionMatrix tM on cCS.MicroSegmentID = tM.MicroSegmentIDFrom and cCS.ActualMonth = tM.ActualMonth " +
      "where tM.MicroSegmentIDFrom <> '" + ChurnColName + "' ")
    segmentFutureData.sort("ClientID").show()
 
    return (segmentFutureData)
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to produce table with customers, segments and probabilities to be there
  def generateCustomersSegmentsFutPast(transitionMatrix: DataFrame, segmentFactData: DataFrame)
  : DataFrame =
  {

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientProbabilityInSegment: Started")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientProbabilityInSegment: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    // Register the tables
    transitionMatrix.registerTempTable("transitionMatrix")
    segmentFactData.registerTempTable("segmentFactData")

    // // Calculate future probabilities of customers in segments
    val segmentFutureData = forecastClientMSProb(transitionMatrix, segmentFactData)

    // // Predict new joiners
    val newJoiners = PredictNewJoiners(transitionMatrix, segmentFactData)

    // Cast the formats for the union
    segmentFactData.registerTempTable("segmentFactData")
    val segmentFactDataFormatted = sqlContext.sql("select ActualMonth, ClientID, cast(Month as date) as Month, MicroSegmentID, Probability from segmentFactData")
    
    // Union the tables
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientProbabilityInSegment: Will join 3 dataframes")
    segmentFactDataFormatted.printSchema()
    segmentFutureData.printSchema()
    newJoiners.printSchema()
    val segmentFutureFactData = segmentFactDataFormatted
      .unionAll(segmentFutureData)
      .unionAll(newJoiners)

    // Save the segmentFutureFactData table 
    if (saveFlag){
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientProbabilityInSegment: Saving segmentFutureFactData")
      var fs: FileSystem = FileSystem.get(new URI(segmentFutureFactDataPath), sc.hadoopConfiguration);
      fs.delete(new Path(segmentFutureFactDataPath), true) // true for recursive
      segmentFutureFactData.coalesce(12).write.format("parquet").save(segmentFutureFactDataPath) //parquet
      FileUtil.chmod(segmentFutureFactDataPath, "999", true)
    }
    
    //    // Export CSV for visualisation
//      val segmentFutureFactDataCSVFormatted = segmentFutureFactData.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
//      segmentFutureFactDataCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/segmentFutureFactDataCSV")     // locally

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientProbabilityInSegment: Finished")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO forecastClientProbabilityInSegment: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    // Return
    return segmentFutureFactData
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to produce range of dates
  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[String] =
    Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to)).map(_.toLocalDate).map(_.toString)

    
    
  //////////////////////////////////////////////////////////////////////////
  // Function to predict dummy clients new joiners and their future facts
  def PredictNewJoiners(TransMatr: DataFrame, FactD: DataFrame)
  : DataFrame =
  {
        
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO PredictNewJoiners: Started")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO PredictNewJoiners: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    // Count average monthly new joiners
    val monthlyNJCount = countMonthlyNewJoiners(FactD)

    // Generate a new joiner. Month, MicroSegmentID, Probability
    val aNewJoinerJourney = generateANewJoinerJourney(TransMatr)
    aNewJoinerJourney.cache()
    aNewJoinerJourney.registerTempTable("aNewJoinerJourney")

    // Multiplicate the newjoiners for a month by number of them to arrive. 
    // Month, MicroSegmentID, Probability, SeqID
    val sqlContextVal = sqlContext
    import sqlContextVal.implicits._
    val SeqIDNJ = sc.makeRDD(1 to monthlyNJCount).toDF("SeqId")
    SeqIDNJ.registerTempTable("SeqIDNJ")
    val newJoinerAMonthJourneys = sqlContext.sql("select * from aNewJoinerJourney join SeqIDNJ")
    newJoinerAMonthJourneys.cache()
    // newJoinerAMonthJourneys.show(20)
    aNewJoinerJourney.coalesce(36)
    newJoinerAMonthJourneys.coalesce(36)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO PredictNewJoiners: SeqIDNJ.count = " + SeqIDNJ.count)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO PredictNewJoiners: aNewJoinerJourney.count = " + aNewJoinerJourney.count)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO PredictNewJoiners: newJoinerAMonthJourneys.count = " + newJoinerAMonthJourneys.count)

    // Generate a range of Months for newJoiners to join
    val dRange = dateRange(
      latestTimeInFacts.plusMonths(1),
      latestTimeInFacts.plusMonths(forecastHorizonTotal),
      Period.months(1)).toList

    // Log
    println("dRange = ", dRange.mkString(", "))

    // Multiplcate newjoiners by months of joining
    // ActualMonth, ClientID, Month, MicroSegmentID, Probability
    val SeqMonthMid = sc.makeRDD(dRange).toDF("SeqMnthMid")
    val SeqMonth = SeqMonthMid.withColumn("SeqMnth", last_day(SeqMonthMid("SeqMnthMid").cast(DateType)))
    SeqMonth.show()
    SeqMonth.registerTempTable("SeqMonth")
    newJoinerAMonthJourneys.registerTempTable("newJoinerAMonthJourneys")
    val newJoinersJourneys = sqlContext.sql("select " +
      "'" + actualMonthDate + "' as ActualMonth, " +
      " cast(newJoinerAMonthJourneys.SeqId as varchar(50)) as SeqId, " +
      " add_months(SeqMonth.SeqMnth, newJoinerAMonthJourneys.Month) as Month, " +
      " SeqMonth.SeqMnth as MonthOfJoining, " +
      " newJoinerAMonthJourneys.MicroSegmentID, " +
      " newJoinerAMonthJourneys.Probability " +
      "from newJoinerAMonthJourneys " +
      "join SeqMonth")
      
    println("line 327")

    // Generate ClientID
    val newJoinersJourneysClID = newJoinersJourneys
      .withColumn("ClientID", concat(lit("NJ_"), newJoinersJourneys("MonthOfJoining"), lit("_"), newJoinersJourneys("SeqId")))

    println("line 333")
    
    // Select the needed columns in required order
    newJoinersJourneysClID.registerTempTable("newJoinersJourneysClID")
    val NJJourneysOrderColumn = sqlContext.sql("select ActualMonth, ClientID, Month, MicroSegmentID, Probability " +
      "from newJoinersJourneysClID")
      .coalesce(36)
      
    println("line 340")

    //NJJourneysOrderColumn.coalesce(36)
    //NJJourneysOrderColumn.repartition(36)
    NJJourneysOrderColumn.cache()
    //NJJourneysOrderColumn.show(5)
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO PredictNewJoiners: NJJourneysOrderColumn.count = " + NJJourneysOrderColumn.count)
    
    // Export .csv for visualisation in Excel. The file will be in one partition
    //    val newJoinersJourneysCSVFormatted = NJJourneysOrderColumn.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
    //    newJoinersJourneysCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/newJoinersJourneysCSV")     // locally

    // Trim the months above those in transition matrix
    FactD.registerTempTable("FactD")
    NJJourneysOrderColumn.registerTempTable("NJJourneysOrderColumn")
    val dateTimeCutOff = (latestTimeInFacts.plusMonths(forecastHorizonTotal)).toLocalDate()
    val dateTimeCutOffEOM = dateTimeCutOff.dayOfMonth().withMaximumValue()
    val newJoinersJourneysTrim = sqlContext.sql("select * from NJJourneysOrderColumn " +
      "where  Month <= '" + dateTimeCutOffEOM.toString() + "'")
      .coalesce(36)
    //newJoinersJourneysTrim.coalesce(36)
    //newJoinersJourneysTrim.repartition(36)
    newJoinersJourneysTrim.cache()
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO PredictNewJoiners: newJoinersJourneysTrim.count = " + newJoinersJourneysTrim.count)

    // Return
    return (newJoinersJourneysTrim)
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to predict dummy clients new joiners and their future facts
  def generateANewJoinerJourney(TransMatr: DataFrame)
  : DataFrame =
  {

    // Make the journey for months >= 1. Month, MicroSegmentID, Probability
    TransMatr.registerTempTable("TransMatr")
    val aNewJoinerJourney = sqlContext.sql(
      "select '" + NewJoinerColName + "' as MicroSegmentID, 0 as Month, 1 as Probability " +
        " union all " +
        "select MicroSegmentIDTo as MicroSegmentID, Month, Probability " +
        " from TransMatr where MicroSegmentIDFrom = '" + NewJoinerColName + "' ")
    aNewJoinerJourney.coalesce(36)
    aNewJoinerJourney.show()

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO generateANewJoinerJourney: Finished")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO generateANewJoinerJourney: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    return (aNewJoinerJourney)
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to initialise all that needed
  def Initialise(args: Array[String]) = 
  {

    // Get end of month date  
    val aTime = Calendar.getInstance()
    aTime.set(Calendar.DAY_OF_MONTH, aTime.getActualMaximum(Calendar.DAY_OF_MONTH))
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    actualMonthDate = formatter.format(aTime.getTime())
    println("Actual Month date " + actualMonthDate )

    // Suppress the logs
    if (!submissionFlag) {
      Logger.getLogger("org").setLevel(Level.OFF) // Option for submission
      Logger.getLogger("akka").setLevel(Level.OFF)
    }

    // Initialize contexts
    var conf: SparkConf = null
    if (!submissionFlag) {
      conf = new SparkConf().setAppName("CLV").setMaster("local") // Option for local execution
    } else {
      conf = new SparkConf().setAppName("CLV"); // Option for submission
    }

    // Spark configuration
    conf.set("spark.io.compression.codec", "lzf")
    conf.set("spark.speculation", "true")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
//    conf.set("spark.executor.memory", "6154m")
//    conf.set("spark.driver.memory", "3g")
//    conf.set("spark.executor.cores", "8")
//    conf.set("spark.cores.max", "infinite")
//    conf.set("spark.yarn.driver.memoryOverhead", "1024")
    conf.set("spark.sql.shuffle.partitions", "36")
    

    // Rewrite the file if exists
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    // Contexts
    sc = new SparkContext(conf);
    sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // File paths
    SegmentDataInPath = args(0)
    SegmentCLVDataOutPath = args(1)
    CustomerSegmentFactInPath = args(2)
    TransitionMatrixOutPath = args(3)
    segmentFutureFactDataPath = args(4)

  }

  
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to calculate CLV for a segment
  def calculateInverseMatDF(InMatrixDF: DataFrame)
  : DataFrame =
  {
      // Log
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateInverseMatDF: Started")
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateInverseMatDF: InMatrixDF.count() = " + InMatrixDF.count())
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateInverseMatDF: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

      // Get distinct labels for indexing
      InMatrixDF.registerTempTable("InMatrixDF")
      val allSegmLabels = sqlContext.sql("select MicroSegmentIDFrom from InMatrixDF union select MicroSegmentIDTo as MicroSegmentIDFrom from InMatrixDF  ")
      allSegmLabels.cache()

      // Indexer
      val Fj_indexer = new StringIndexer()
        .setInputCol("MicroSegmentIDFrom")
        .setOutputCol("j")
        .fit(allSegmLabels)

      // Inverse indexer
      val InvIndexer_jF = new IndexToString()
        .setInputCol("j")
        .setOutputCol("MicroSegmentIDFrom")
        .setLabels(Fj_indexer.labels)

      // Transform the columns into indices 
      val DM_i = Fj_indexer
        .transform(InMatrixDF)
        .drop("MicroSegmentIDFrom")
        .withColumnRenamed("j", "i")
        .withColumnRenamed("MicroSegmentIDTo", "MicroSegmentIDFrom")

      val DM_ijv = Fj_indexer
        .transform(DM_i)
        .drop("MicroSegmentIDFrom")
        .withColumnRenamed("Probability", "value")
        
        

      DM_ijv.registerTempTable("DM_ijv")
      val DM_ijv_order = sqlContext.sql("select floor(i) as i, floor(j) as j, value from DM_ijv order by i")

      // Create an RDD[MatrixEntry]
      val RddME = DM_ijv_order.map(p => MatrixEntry(p.getLong(0), p.getLong(1), p.getDouble(2)))

      // Create a coordinate matrix
      val mat: CoordinateMatrix = new CoordinateMatrix(RddME)

      // Compute Row Matrix
      val rowMatrix = mat.toIndexedRowMatrix()  
      
      // Calculate inverse of the discounted matrix (I-TM/(1+d))^(-1)
      val InverseMatrix = computeInverse(rowMatrix)
 
      // TEST if the matrix really inverse. The mustUnity must be unit matrix!!
      val mustUnity = rowMatrix.multiply(InverseMatrix)
      // DEBUG LINE mustUnity.rows.toArray()

      
      // Convert dense matrix to the Sparse one
      val InvMatCM = InverseMatrix.toSparse

      // TODO: A more robust method for indicesTuples calculation! That would FAIL on sparse matrix!!
      // Create a DataFrame from the Sparse matrix
      val sqlContextVal = sqlContext
      import sqlContextVal.implicits._
      val indicesTuples = (0 to (InvMatCM.numActives - 1)).map(p => ((p - InvMatCM.rowIndices(p)) / InverseMatrix.numCols, InvMatCM.rowIndices(p)))
      val InvMatInd_DF = indicesTuples.map(p => (p._1, p._2, InvMatCM.apply(p._1, p._2))).toDF("i", "j", "value")
      InvMatInd_DF.cache()

      // Assign back Segment Labels instead of indices
      val InvMatrix_i = InvIndexer_jF
        .transform(InvMatInd_DF)
        .drop("j")
        .withColumnRenamed("i", "j")
        .withColumnRenamed("MicroSegmentIDFrom", "MicroSegmentIDTo")

      val InvDiscMatrix = InvIndexer_jF
        .transform(InvMatrix_i)
        .drop("j")
      InvDiscMatrix.cache()
      
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateInverseMatDF: InvDiscMatrix.count() = " + InvDiscMatrix.count())


      // Return
      return (InvDiscMatrix)

    }

  //////////////////////////////////////////////////////////////////////////
  // Function to calculate CLV for a segment
  def calculateSegmentCLV(transMatrix: DataFrame, dSegments: DataFrame): DataFrame =
    {

      // Log
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentCLV: Started")
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentCLV: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")
      dSegments.printSchema()

      // Extract the 1M matrix corresponding to ActualMonth
      transMatrix.registerTempTable("transMatrix")
      val TM1M = transMatrix.filter("Month = 1").drop("Month").filter("ActualMonth = '" + actualMonthDate + "' ").drop("ActualMonth")
      TM1M.cache()

      // Get distinct categories from transition matrix   
      TM1M.registerTempTable("TM1M")
      val allSegmLabels = sqlContext.sql("select MicroSegmentIDFrom from TM1M union select MicroSegmentIDTo as MicroSegmentIDFrom from TM1M  ")
      allSegmLabels.cache()

      // Create a unit matrix
      allSegmLabels.registerTempTable("allSegmLabels")
      val unitMatrix = sqlContext.sql("select distinct MicroSegmentIDFrom, MicroSegmentIDFrom as MicroSegmentIDTo, 1.0 as Probability from allSegmLabels ")
      unitMatrix.cache()

      // Calculate discounted matrix (I-TM/(1+d))
      unitMatrix.registerTempTable("unitMatrix")
      val discountedMatrix = sqlContext.sql("select coalesce(TM.MicroSegmentIDFrom, UM.MicroSegmentIDFrom) as MicroSegmentIDFrom, " +
        "coalesce(TM.MicroSegmentIDTo, UM.MicroSegmentIDTo) as MicroSegmentIDTo, " +
        " coalesce(UM.Probability, 0) - coalesce(TM.Probability, 0)/ " + (1.0 + discountFactor) + " as value " +
        "from unitMatrix UM " +
        "full outer join TM1M TM " +
        " on TM.MicroSegmentIDFrom = UM.MicroSegmentIDFrom and TM.MicroSegmentIDTo = UM.MicroSegmentIDTo ")
      discountedMatrix.cache()
      discountedMatrix.printSchema()


      // Calculate inverse matrix from matrix in DataFrame coordinate format 
      discountedMatrix.printSchema()
      val InvDiscMatrix = calculateInverseMatDF(discountedMatrix)

      // CSV
      //    val InvMatrixCSVFormatted = InvDiscMatrix.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
      //    InvMatrixCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/InvMatrixCSV")     // locally

//          // TEST the matrix is really inverse
//          InvDiscMatrix.registerTempTable("InvMatrix")
//          discountedMatrix.registerTempTable("discountedMatrix")
//          val MatInvProd = sqlContext.sql("select I.MicroSegmentIDTo, M.MicroSegmentIDFrom, sum(I.value*M.value) as value " + 
//              "from discountedMatrix M join InvMatrix I " + 
//              "on M.MicroSegmentIDTo = I.MicroSegmentIDFrom " + 
//              "group by I.MicroSegmentIDTo, M.MicroSegmentIDFrom")
////          val MatInvProd = sqlContext.sql("select I.MicroSegmentIDTo, M.MicroSegmentIDTo, sum(I.value*M.value) as value " + 
////              "from discountedMatrix M join InvMatrix I " + 
////              "on M.MicroSegmentIDFrom = I.MicroSegmentIDFrom " + 
////              "group by I.MicroSegmentIDTo, M.MicroSegmentIDTo")
//          MatInvProd.cache()
//          // MatInvProd.show()
//          val MatInvProdCSVFormatted = MatInvProd.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
//          MatInvProdCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/InvMatProdCSV")     // locally

      // Make the revenues of absorbing segments zero - otherwise their CLV explodes
      dSegments.registerTempTable("dSegments")
      val dSegments0RevLeave = sqlContext.sql("select ActualMonth, MicroSegmentID, RevenueFrc from dSegments " + 
        "where MicroSegmentID <> '" + ChurnColName + "' " +
        " union select ActualMonth, MicroSegmentID, 0 as RevenueFrc from dSegments " + 
        "where MicroSegmentID = '" + ChurnColName + "' ")
      dSegments0RevLeave.show(300)
      
      //    val dSegments0RevLeaveCSVFormatted = dSegments0RevLeave.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
      //    dSegments0RevLeaveCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/dSegments0RevLeave")     // locally

      // Calculate CLV by multiplying revenue vector by the inverse Discounted matrix
      dSegments0RevLeave.registerTempTable("dSegments0RevLeave")
      InvDiscMatrix.registerTempTable("InvMatrix")
      val dSegmentCLV = sqlContext.sql("select ds.ActualMonth, im.MicroSegmentIDFrom as MicroSegmentID, sum(ds.RevenueFrc * im.value) as CLV " +
        " from dSegments0RevLeave ds " +
        "join InvMatrix im on ds.MicroSegmentID = im.MicroSegmentIDTo " +
        "group by ds.ActualMonth, im.MicroSegmentIDFrom ")
      dSegmentCLV.show(100)

      // Save the CLV matrix
      if (saveFlag){
        println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentCLV: Saving dSegmentCLV")
        var fs: FileSystem = FileSystem.get(new URI(SegmentCLVDataOutPath), sc.hadoopConfiguration);
        fs.delete(new Path(SegmentCLVDataOutPath), true) // true for recursive
        dSegmentCLV.coalesce(12).write.format("parquet").save(SegmentCLVDataOutPath) //parquet
        FileUtil.chmod(SegmentCLVDataOutPath, "999", true)
      }
      
      // CSV
      //    val dSegmentCLVCSVFormatted = dSegmentCLV.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
      //    dSegmentCLVCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/dSegmentCLV")     // locally

      // Log
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentCLV: Finished")
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateSegmentCLV: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

      return (dSegmentCLV)
    }

  //////////////////////////////////////////////////////////////////////////
  // Function to get the segment dimension data
  def getSegmentsDimensionData(): DataFrame = {
    val outData = sqlContext.sql("SELECT * FROM parquet.`" + SegmentDataInPath + "`")
    outData.coalesce(36).persist()
    
    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO getSegmentsDimensionData: Got the data")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO getSegmentsDimensionData: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    return (outData)
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to get the customer segment fact data
  def getSegmentFactData()
  : DataFrame = 
  {

    // Load the file
    val data = sqlContext.sql("SELECT * FROM parquet.`" + CustomerSegmentFactInPath + "`").coalesce(8)
    var data2 = data

    // Get the latest record in fact table
    data2.registerTempTable("data2")
    latestTimeInFacts = new DateTime(sqlContext.sql("select cast(max(Month) as date) from data2").first().getDate(0))

    // Repartition
    println("data.rdd.partitions.size = " + data.rdd.partitions.size)
    data2.coalesce(36).cache()

    // Log
    println("latestTimeInFacts = " + latestTimeInFacts.toLocalDate().toString())
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO getSegmentsData: Got the data")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO getSegmentsData: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    return data2
  }

  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to calculate the transition matrix on several months
  def calculateTransitionMatrix(segmentFactData: DataFrame)
  : DataFrame = 
  {

    // Calculate on-month transition matrix
    val transMatrix1M = calculateTransitionMatrix1M(segmentFactData)

    // Produce multiple months transition matrices in a loop
    transMatrix1M.registerTempTable("transMatrix1M")
    var transMatrix = transMatrix1M.withColumn("Month", lit(1)) // Initialise the accumulation table
    transMatrix.cache()
    var actualTransMatrix = transMatrix // Initialise the actual transition table - the skin layer of the accumulation table 
    actualTransMatrix.cache()

    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix: Start the for loop")
    for (forecHorizon <- 2 to forecastHorizonTotal) {

      // Register the tables
      transMatrix.registerTempTable("transMatrix")
      actualTransMatrix.registerTempTable("actualTransMatrix")
 
      // Calculate actualTransMatrix by its multiplication by the transMatrix1M
      actualTransMatrix = sqlContext.sql("select " +
        "tmCur.ActualMonth, tmTot.MicroSegmentIDFrom, tmCur.MicroSegmentIDTo, sum(tmTot.Probability*tmCur.Probability) as Probability " +
        "from actualTransMatrix tmTot join transMatrix1M tmCur on tmTot.MicroSegmentIDTo = tmCur.MicroSegmentIDFrom and tmTot.ActualMonth = tmCur.ActualMonth " +
        "group by tmCur.ActualMonth, tmTot.MicroSegmentIDFrom, tmCur.MicroSegmentIDTo ")
      actualTransMatrix.cache()
      // actualTransMatrix.show(20) 

      // Add the Month value
      val actualTransMatrix_wMonth = actualTransMatrix.withColumn("Month", lit(forecHorizon))
      actualTransMatrix_wMonth.registerTempTable("actualTransMatrix_wMonth")
      actualTransMatrix_wMonth.cache()

      // Add the matrix for actual forecHorizon+1 to the accumulation table. VERY SLOW 
      transMatrix = transMatrix.unionAll(actualTransMatrix_wMonth)
      transMatrix.cache()
  
      // Log
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix: Transition matrix for " + forecHorizon + " months")
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix: From beginning " +
        (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    }

    // Log
    transMatrix.cache()
    transMatrix.show()

    // Save the transition matrix
    if (saveFlag){
      println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix: Saving transMatrix")
      var fs: FileSystem = FileSystem.get(new URI(TransitionMatrixOutPath), sc.hadoopConfiguration);
      fs.delete(new Path(TransitionMatrixOutPath), true) // true for recursive
      transMatrix.coalesce(12).write.format("parquet").save(TransitionMatrixOutPath) //parquet
      FileUtil.chmod(TransitionMatrixOutPath, "999", true)
    }
    
    //    // Export .csv for visualisation in Excel. The file will be in one partition
    //    val transitionMatrixCSVFormatted = transMatrix.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
    //    transitionMatrixCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/TransitionMatrixCSV")     // locally

    //Log 
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix: Saved the transition matrix for several months")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    // Output the transition matrix
    return transMatrix
  }

  
  
  
  //////////////////////////////////////////////////////////////////////////
  // Function to calculate the transition matrix on one month
  def calculateTransitionMatrix1M(segmFactData: DataFrame)
  : DataFrame = 
  {

    // Transform into transitions between segments
    segmFactData.registerTempTable("segmFactData")
    segmFactData.printSchema()
    val TransitionsData = sqlContext.sql("select " +
      "tFrom.ClientID as ClientIdFrom, tTo.ClientID as ClientIDTo, " +
      "tFrom.Month as MonthFrom, tTo.Month as MonthTo, " +
      "tFrom.MicroSegmentID as MicroSegmentIDFrom, tTo.MicroSegmentID as MicroSegmentIDTo " +
      "from segmFactData tFrom " +
      "join segmFactData tTo on tFrom.ClientID = tTo.ClientID and add_months(cast(tFrom.Month as date), 1) = cast(tTo.Month as date)")
    TransitionsData.cache()
    TransitionsData.show()
    println("segmFactData count = " + segmFactData.count() + ", TransitionsData count = " + TransitionsData.count())

    // Count the transitions
    TransitionsData.registerTempTable("TransitionsData")
    val transitionCounts = sqlContext.sql("select " +
      "MicroSegmentIDFrom, MicroSegmentIDTo, count(1) Count " +
      "from TransitionsData " +
      "group by MicroSegmentIDFrom, MicroSegmentIDTo")
    transitionCounts.cache()
    transitionCounts.show()

    // Count the total transitions from a segment
    transitionCounts.registerTempTable("transitionCounts")
    val transitionFromCounts = sqlContext.sql("select MicroSegmentIDFrom, sum(Count) as TotalCount from transitionCounts group by MicroSegmentIDFrom")
    transitionFromCounts.cache()
    transitionFromCounts.show()

    // Normalize to make probabilities
    transitionFromCounts.registerTempTable("transitionFromCounts")
    val transitionMatrix = sqlContext.sql("select '" + actualMonthDate + "' as ActualMonth, " +
      "tc.MicroSegmentIDFrom, tc.MicroSegmentIDTo, tc.Count/tfc.TotalCount as Probability " +
      "from transitionCounts tc " +
      "join transitionFromCounts tfc on tc.MicroSegmentIDFrom = tfc.MicroSegmentIDFrom")
    transitionMatrix.cache()


    // Add formal lines for accumulating segments
    // Each of the segment should be present in the 'from' column, even with unity probability to stay as itself
    println("Adding lines for accumulation segments, actualMonthDate = " + actualMonthDate)
    transitionMatrix.registerTempTable("transitionMatrix")
    val accumulationSegments = sqlContext.sql("select distinct t.MicroSegmentIDTo as MicroSegmentID from transitionMatrix t " + 
        "left join transitionMatrix f on f.MicroSegmentIDFrom = t.MicroSegmentIDTo " + 
        "where f.MicroSegmentIDFrom is null")
        
    println("Accumulation segments are: ")
    accumulationSegments.show()
      
    val leaveSegmentList = Array(ChurnColName)
    val lSegmCount = leaveSegmentList.size
    val actualMonthDate2 = actualMonthDate    
    
    val leaveSegmTransMatrixRows = accumulationSegments
      .withColumn("ActualMonth", lit(actualMonthDate2))
      .withColumn("MicroSegmentIDFrom", accumulationSegments("MicroSegmentID"))
      .withColumn("MicroSegmentIDTo", accumulationSegments("MicroSegmentID"))
      .withColumn("Probability", lit("1").cast(DoubleType))
      .drop("MicroSegmentID")

//    val sqlContextVal = sqlContext
//    import sqlContextVal.implicits._
//    val leaveSegmTransMatrixRows = sc.makeRDD(1 to lSegmCount)
//      .map(i => (actualMonthDate2, leaveSegmentList(i - 1), leaveSegmentList(i - 1), 1))
//      .toDF("ActualMonth", "MicroSegmentIDFrom", "MicroSegmentIDTo", "Probability")
//    leaveSegmTransMatrixRows.show()
    
    println("Merging the following tables:")
    leaveSegmTransMatrixRows.printSchema()
    transitionMatrix.printSchema()

    val transitionMatrixLS = leaveSegmTransMatrixRows.unionAll(transitionMatrix)

    // Cache
    transitionMatrixLS.cache()
    transitionMatrixLS.registerTempTable("transitionMatrixLS")
    sqlContext.sql("select * from transitionMatrixLS order by Probability desc").show()
    
    // Export .csv for visualisation in Excel. The file will be in one partition
//        val transitionMatrixCSVFormatted = transitionMatrixLS.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true") 
//        transitionMatrixCSVFormatted.save("C:/Work/Projects/C360/Data/MarkovChain/TransitionMatrixCSV")     // locally

    // Log
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix1M: One-month transition matrix calculated")
    println(Calendar.getInstance().getTime().toLocaleString() + " INFO calculateTransitionMatrix1M: From beginning " +
      (Calendar.getInstance().getTime().getTime() - startTime.getTime()) / 1000.0 + " seconds")

    // Return
    return transitionMatrixLS
  }

}
