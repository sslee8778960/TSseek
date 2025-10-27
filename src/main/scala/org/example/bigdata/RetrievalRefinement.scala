package org.example.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.util.SerializableConfiguration
import java.sql.{Connection, DriverManager}
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedReader, InputStreamReader}
import scala.util.control.NonFatal

object RetrievalRefinement {

  @transient private lazy val logger = LoggerFactory.getLogger(RetrievalRefinement.getClass)



  val DATASET = Config.DATASET
  val DATASET_SIZE = Config.DATASET_SIZE
  val ALPHA = Config.ALPHA

  val datasetParams = Config.getParams(DATASET, ALPHA)
  val QUERY_PATTERN = datasetParams.queryPattern

  val DB_NAME = "postgres"
  val DB_USER = "postgres"
  val DB_PASSWORD = "YOUR_PASSWORD_HERE"
  val TABLE_PREFIX = s"${DATASET.toLowerCase}_${DATASET_SIZE}_og_ts"

  val NUM_TABLES = DATASET_SIZE match {
    case "25m" => 40
    case "50m" => 80
    case "100m" => 160
    case "200m" => 320
    case _ => throw new IllegalArgumentException(s"Unknown dataset size: $DATASET_SIZE")
  }

  val tableToMachine: Map[Int, String] =
    (1 to NUM_TABLES/2).map(_ -> "shark1").toMap ++
    ((NUM_TABLES/2 + 1) to NUM_TABLES).map(_ -> "shark2").toMap

  val BATCH_SIZE = 10000

  val DFA_TOLERANCE = 0.0001


  sealed trait Element extends Serializable {
    def matches(value: Double): Boolean
  }

  case class NumberElement(value: Double) extends Element {
    def matches(number: Double): Boolean = math.abs(value - number) < DFA_TOLERANCE
  }

  case class IntervalElement(start: Double, end: Double,
                             startInclude: Boolean = true,
                             endInclude: Boolean = true) extends Element {
    def matches(number: Double): Boolean = {
      val atStart = math.abs(number - start) < DFA_TOLERANCE
      val atEnd = math.abs(number - end) < DFA_TOLERANCE

      if (math.abs(start - end) < DFA_TOLERANCE) {
        return atStart || atEnd
      }

      if (startInclude && endInclude) {
        (start - DFA_TOLERANCE) <= number && number <= (end + DFA_TOLERANCE)
      } else if (!startInclude && endInclude) {
        (start + DFA_TOLERANCE) < number && number <= (end + DFA_TOLERANCE)
      } else if (startInclude && !endInclude) {
        (start - DFA_TOLERANCE) <= number && number < (end - DFA_TOLERANCE)
      } else {
        (start + DFA_TOLERANCE) < number && number < (end - DFA_TOLERANCE)
      }
    }
  }

  case class RandomElement() extends Element {
    def matches(number: Double): Boolean = true
  }

  sealed trait GrowthType extends Serializable
  case object Increase extends GrowthType
  case object Decrease extends GrowthType
  case object Equal extends GrowthType
  case object Random extends GrowthType

  case class Growth(growthType: GrowthType, amount: Double) extends Serializable {
    def matches(base: Double, next: Double): Boolean = growthType match {
      case Increase => math.abs((base + amount) - next) < DFA_TOLERANCE
      case Decrease => math.abs((base - amount) - next) < DFA_TOLERANCE
      case Equal => math.abs(base - next) < DFA_TOLERANCE
      case Random => true
    }
  }

  sealed trait Module extends Serializable {
    def minLength: Int
    def maxLength: Int
    def matches(values: Array[Double]): Boolean
  }

  case class NumberModule(elements: Array[Element]) extends Module {
    val minLength: Int = elements.length
    val maxLength: Int = elements.length

    def matches(values: Array[Double]): Boolean = {
      if (values.length != elements.length) return false
      values.zip(elements).forall { case (value, element) => element.matches(value) }
    }
  }

  case class PatternModule(valueInterval: IntervalElement,
                          growth: Growth,
                          minGrowLength: Int,
                          maxGrowLength: Int) extends Module {
    val minLength: Int = minGrowLength
    val maxLength: Int = maxGrowLength

    def matches(values: Array[Double]): Boolean = {
      if (values.length < minGrowLength || values.length > maxGrowLength) return false

      if (!valueInterval.matches(values(0))) return false

      for (i <- 1 until values.length) {
        if (!valueInterval.matches(values(i)) ||
            !growth.matches(values(i-1), values(i))) {
          return false
        }
      }
      true
    }
  }


  def main(args: Array[String]): Unit = {
    val overallStartTime = System.nanoTime()

    val conf = new SparkConf()
      .setAppName(s"UnifiedRetrievalRefinement_${DATASET}_${DATASET_SIZE}")
      .set("spark.executor.memory", "10g")
      .set("spark.executor.cores", "4")
      .set("spark.cores.max", NUM_TABLES.toString)
      .set("spark.default.parallelism", NUM_TABLES.toString)
      .set("spark.sql.shuffle.partitions", NUM_TABLES.toString)
      .set("spark.network.timeout", "800s")
      .set("spark.executor.heartbeatInterval", "60s")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    try {
      println("\n" + "=" * 80)
      println(s"UNIFIED RETRIEVAL + DFA REFINEMENT PIPELINE")
      println(s"Dataset: $DATASET $DATASET_SIZE")
      println(s"Alpha: $ALPHA")
      println("=" * 80)

      val alphaStr = f"$ALPHA%.2f"
      val queryResultsPath = DATASET match {
        case "TSBS" => s"hdfs://shark1local:9000/user/xli3/output/tsbs/query_results_alpha_${alphaStr}"
        case "ECG" => s"hdfs://shark1local:9000/user/xli3/output/ecg/query_results_alpha_${alphaStr}"
        case "RANDOMWALK" => s"hdfs://shark1local:9000/user/xli3/output/random_walk/query_results_alpha_${alphaStr}"
        case _ => throw new IllegalArgumentException(s"Unknown dataset: $DATASET")
      }

      println(s"\nReading query results from: $queryResultsPath")
      println(s"Processing $NUM_TABLES tables (based on dataset size: $DATASET_SIZE)")

      val queryFiles = (1 to NUM_TABLES).map { tableNum =>
        val fileName = s"query_results_table${tableNum}.csv"
        val filePath = s"$queryResultsPath/$fileName"
        (tableNum, filePath)
      }

      println(s"Found ${queryFiles.size} query result files")

      println("\n" + "=" * 80)
      println("STEP 1: Parallel Retrieval + DFA Refinement")
      println("=" * 80)

      val tableTaskPairs = queryFiles.map { case (tableNum, queryFilePath) =>
        val machine = tableToMachine(tableNum)
        val pgTableName = s"${TABLE_PREFIX}_table${tableNum}"
        (tableNum, queryFilePath, machine, pgTableName)
      }

      val hadoopConfBroadcast = spark.sparkContext.broadcast(new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))

      val results = spark.sparkContext
        .parallelize(tableTaskPairs, tableTaskPairs.size)
        .mapPartitions { partitionIter =>
          val hadoopConf = hadoopConfBroadcast.value.value
          val fs = FileSystem.newInstance(hadoopConf)
          try {
            partitionIter.map { case (tableNum, queryFilePath, machine, pgTableName) =>
              processTable(fs, tableNum, queryFilePath, machine, pgTableName)
            }.toList.iterator
          } finally {
            fs.close()
          }
        }
        .collect()
        .sortBy(_._1)

      println("\n" + "=" * 80)
      println("STEP 2: Aggregating Results")
      println("=" * 80)

      val totalCandidates = results.map(_._2).sum
      val totalMatches = results.map(_._3).sum
      val totalRetrievalTime = if (results.nonEmpty) results.map(_._4).sum / results.length else 0.0
      val totalRefinementTime = if (results.nonEmpty) results.map(_._5).sum / results.length else 0.0

      val allMatchingData = results.flatMap(_._6).toMap

      println(f"Total candidates retrieved: $totalCandidates%,d")
      println(f"Total matches found: $totalMatches%,d")
      println(f"Average retrieval time per table: $totalRetrievalTime%.2f seconds")
      println(f"Average refinement time per table: $totalRefinementTime%.2f seconds")
      val matchRate = if (totalCandidates == 0) 0.0 else totalMatches.toDouble / totalCandidates * 100.0
      println(f"Match rate: $matchRate%.2f%%")

      if (allMatchingData.nonEmpty) {
        println("\n" + "=" * 80)
        println("STEP 3: Saving Results to HDFS")
        println("=" * 80)

        val timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
        val alphaStr = f"$ALPHA%.2f"
        val outputPath = DATASET match {
          case "TSBS" => s"hdfs://shark1local:9000/user/xli3/output/tsbs/dfa_matches_alpha_${alphaStr}_${timestamp}"
          case "ECG" => s"hdfs://shark1local:9000/user/xli3/output/ecg/dfa_matches_alpha_${alphaStr}_${timestamp}"
          case "RANDOMWALK" => s"hdfs://shark1local:9000/user/xli3/output/random_walk/dfa_matches_alpha_${alphaStr}_${timestamp}"
          case _ => s"hdfs://shark1local:9000/user/xli3/output/dfa_matches_alpha_${alphaStr}_${timestamp}"
        }

        println(s"Saving ${allMatchingData.size} matching time series (ID + data) to: $outputPath")

        val matchingDataRDD = spark.sparkContext.parallelize(allMatchingData.toSeq.sortBy(_._1))
          .map { case (tsId, tsData) => s"$tsId;$tsData" }

        matchingDataRDD.saveAsTextFile(outputPath)

        println(s"✓ Results saved to HDFS: $outputPath")
      } else {
        println("\nNo matches found, skipping HDFS save.")
      }

      val overallEndTime = System.nanoTime()
      val totalDuration = (overallEndTime - overallStartTime) / 1e9d / 60.0 // minutes

      println("\n" + "=" * 80)
      println(s"✓ PIPELINE COMPLETE!")
      println(f"Total time: $totalDuration%.2f minutes")
      println("=" * 80)

    } finally {
      spark.stop()
    }
  }

  def loadCandidateIds(fs: FileSystem, queryFilePath: String): Array[Long] = {
    val path = new Path(queryFilePath)
    if (!fs.exists(path)) {
      println(s"[Candidates] File not found: $queryFilePath")
      return Array.emptyLongArray
    }

    val buffer = ArrayBuffer[Long]()
    var reader: BufferedReader = null

    try {
      reader = new BufferedReader(new InputStreamReader(fs.open(path)))
      var line = reader.readLine()
      while (line != null) {
        val trimmed = line.trim
        if (trimmed.nonEmpty && trimmed != "time_series_id" && !trimmed.startsWith("time_series_id")) {
          try {
            buffer += trimmed.toLong
          } catch {
            case NonFatal(e) =>
              println(s"[Candidates] Skipping invalid value '$trimmed' in $queryFilePath: ${e.getMessage}")
          }
        }
        line = reader.readLine()
      }
    } finally {
      if (reader != null) {
        reader.close()
      }
    }

    buffer.distinct.toArray
  }

  def processTable(fs: FileSystem, tableNum: Int, queryFilePath: String, machine: String, pgTableName: String): (Int, Long, Long, Double, Double, Map[Long, String]) = {
    val startTime = System.nanoTime()

    println(s"\n[Table $tableNum on $machine] Starting processing...")

    try {
      println(s"[Table $tableNum] Reading candidates from HDFS...")

      val candidateIds = loadCandidateIds(fs, queryFilePath)

      println(s"[Table $tableNum] Found ${candidateIds.length} candidates")

      if (candidateIds.isEmpty) {
        println(s"[Table $tableNum] No candidates, skipping...")
        return (tableNum, 0L, 0L, 0.0, 0.0, Map.empty[Long, String])
      }

      val retrievalStartTime = System.nanoTime()
      println(s"[Table $tableNum] Retrieving time series data from PostgreSQL...")

      val (matchingData, retrievalTime, refinementTime) =
        retrieveAndRefine(machine, pgTableName, candidateIds)

      val endTime = System.nanoTime()
      val totalTime = (endTime - startTime) / 1e9d
      println(f"[Table $tableNum%d] ✓ Complete in $totalTime%.2f seconds")

      (tableNum, candidateIds.length.toLong, matchingData.size.toLong, retrievalTime, refinementTime, matchingData)

    } catch {
      case e: Exception =>
        println(s"[Table $tableNum] ✗ ERROR: ${e.getMessage}")
        e.printStackTrace()
        (tableNum, 0L, 0L, 0.0, 0.0, Map.empty[Long, String])
    }
  }

  def retrieveAndRefine(machine: String,
                        tableName: String,
                        candidateIds: Array[Long]): (Map[Long, String], Double, Double) = {
    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    val matching = scala.collection.mutable.Map[Long, String]()

    val batches = candidateIds.grouped(BATCH_SIZE)
    println(s"  Processing ${candidateIds.length / BATCH_SIZE + 1} batches of size up to $BATCH_SIZE...")

    var conn: Connection = null
    var stmt: java.sql.PreparedStatement = null

    var totalRetrievalNanos = 0L
    var totalRefinementNanos = 0L

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      stmt = conn.prepareStatement(s"SELECT ts_id, ts_data FROM $tableName WHERE ts_id = ANY (?)")

      var batchIdx = 0
      batches.foreach { batch =>
        val startRetrieve = System.nanoTime()
        val sqlArray = conn.createArrayOf("BIGINT", batch.map(Long.box))
        stmt.setArray(1, sqlArray)
        val rs = stmt.executeQuery()
        val retrievedBatch = scala.collection.mutable.ArrayBuffer[(Long, String)]()
        while (rs.next()) {
          retrievedBatch += rs.getLong("ts_id") -> rs.getString("ts_data")
        }
        rs.close()
        sqlArray.free()
        totalRetrievalNanos += System.nanoTime() - startRetrieve

        val startRefine = System.nanoTime()
        retrievedBatch.foreach { case (tsId, tsData) =>
          if (matchPattern(tsData)) {
            matching += tsId -> tsData
          }
        }
        totalRefinementNanos += System.nanoTime() - startRefine

        batchIdx += 1
        if (batchIdx % 10 == 0) print(".")
      }
      println()

    } catch {
      case e: Exception =>
        if (conn != null) conn.close()
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }

    (matching.toMap, totalRetrievalNanos / 1e9d, totalRefinementNanos / 1e9d)
  }

  def parseTimeSeries(tsString: String): Array[Double] = {
    try {
      val pattern = """MULTIPOINT\(\((.*)\)\)""".r
      tsString match {
        case pattern(points) =>
          points.split("\\), \\(").map { point =>
            val parts = point.trim.split("\\s+")
            if (parts.length >= 2) parts(1).toDouble
            else throw new Exception(s"Invalid point format: $point")
          }
        case _ =>
          logger.warn(s"Invalid time series format: ${tsString.take(50)}...")
          Array.empty[Double]
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error parsing time series: ${e.getMessage}")
        Array.empty[Double]
    }
  }

  def createPatternModules(): Array[Module] = {
    val modules = ArrayBuffer[Module]()

    val parsedElements = QueryParser.parsePattern(QUERY_PATTERN)

    parsedElements.foreach { case (leftWindow, rightWindow, flag, constraintsOpt) =>
      flag match {
        case subpattern if subpattern.startsWith("|") && subpattern.endsWith("|") =>
          constraintsOpt match {
            case Some(constraints) =>
              val (yMin, yMax) = constraints.valueRange
              val length = constraints.length
              val direction = constraints.direction.getOrElse("ANY")

              
              val growth = direction match {
                case "INCREASING" =>
                  val growthAmount = extractGrowthAmount(subpattern)
                  Growth(Increase, growthAmount)
                case "DECREASING" =>
                  val growthAmount = extractGrowthAmount(subpattern)
                  Growth(Decrease, growthAmount)
                case "FLAT" =>
                  Growth(Equal, 0.0)
                case "ANY" =>
                  Growth(Random, 0.0)
                case _ =>
                  Growth(Random, 0.0)
              }

              modules += PatternModule(
                IntervalElement(yMin, yMax),
                growth,
                length, length
              )

            case None =>
              logger.warn(s"Subpattern missing constraints: $subpattern")
          }

        case "nonsubpattern" =>
          val (_, yMin, _, yMax) = leftWindow
          // Single value element
          modules += NumberModule(Array(IntervalElement(yMin, yMax)))

        case _ =>
          logger.warn(s"Unknown flag: $flag")
      }
    }

    modules.toArray
  }

  def extractGrowthAmount(subpattern: String): Double = {
    val growthRegex = """\*([0-9.]+)\*""".r
    growthRegex.findFirstMatchIn(subpattern) match {
      case Some(m) => m.group(1).toDouble
      case None => 0.0
    }
  }

  def matchHelper(values: Array[Double], modules: Array[Module],
                 valueIdx: Int, moduleIdx: Int): Boolean = {
    if (valueIdx == values.length && moduleIdx == modules.length) return true
    if (valueIdx >= values.length || moduleIdx >= modules.length) return false

    val module = modules(moduleIdx)
    val delta = module.maxLength - module.minLength + 1

    for (i <- 0 until delta) {
      val length = module.minLength + i
      if (valueIdx + length <= values.length) {
        val slice = values.slice(valueIdx, valueIdx + length)
        if (module.matches(slice)) {
          if (matchHelper(values, modules, valueIdx + length, moduleIdx + 1)) {
            return true
          }
        }
      }
    }
    false
  }

  def matchPattern(tsString: String): Boolean = {
    val values = parseTimeSeries(tsString)
    if (values.isEmpty) return false

    val modules = createPatternModules()

    val maxLen = modules.map(_.maxLength).sum
    val minLen = modules.map(_.minLength).sum

    if (values.length > maxLen || values.length < minLen) {
      return false
    }

    matchHelper(values, modules, 0, 0)
  }

  def performDFARefinement(timeSeriesData: Map[Long, String]): Set[Long] = {
    timeSeriesData.iterator.collect { case (tsId, tsData) if matchPattern(tsData) => tsId }.toSet
  }
}
