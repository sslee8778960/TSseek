package org.example.bigdata

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date
import java.io._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SubsequenceQueryProcessing {

  private val logger = LoggerFactory.getLogger(getClass)

  case class Point(x: Double, y: Double)

  case class GridCell(xBL: Double, yBL: Double, xTR: Double, yTR: Double, machine: String) {
    def grid_id: String = s"${xBL}_${yBL}_${xTR}_${yTR}"
  }


  def generateGrid(w: Int, h: Double, yMin: Double, yMax: Double): Array[GridCell] = {
    val xMin = 0; val xMax = 128
    val machines = Array("shark1", "shark2")
    val xSteps = (xMax - xMin) / w
    val ySteps = ((yMax - yMin) / h).toInt

    (for {
      i <- 0 until xSteps
      j <- 0 until ySteps
    } yield {
      val xBL = xMin + i * w
      val yBL = yMin + j * h
      val xTR = xBL + w
      val yTR = yBL + h
      val machineIndex = (i * ySteps + j) % machines.length
      GridCell(xBL, yBL, xTR, yTR, machines(machineIndex))
    }).toArray
  }

  def readMapFromHDFS[T, U](filePath: String,
                            spark: SparkSession,
                            keyCast: String => T,
                            valueCast: String => U): Map[T, U] = {
    val rdd = spark.sparkContext.textFile(filePath)
    val pairs = rdd.collect().flatMap { line =>
      line.split(",").toList match {
        case k :: v :: Nil => Some(keyCast(k) -> valueCast(v))
        case _             => None
      }
    }
    pairs.toMap
  }

  def isIntersecting(q: (Double, Double, Double, Double), cell: GridCell): Boolean = {
    val (llx, lly, urx, ury) = q
    !(urx < cell.xBL || llx > cell.xTR || ury < cell.yBL || lly > cell.yTR)
  }


  def generateSlopeConstraint(direction: Option[String]): String = {
    direction match {
      case Some("INCREASING") => " AND segment_slope > 0"
      case Some("DECREASING") => " AND segment_slope < 0"
      case Some("FLAT") => " AND segment_slope = 0"
      case Some("ANY") | None => ""
      case _ => ""
    }
  }

  def generateFluctuationConstraint(directionConstraint: Option[String]): String = {
    if (!Config.USE_FLUCTUATION_FILTER) return ""
    directionConstraint match {
      case Some("ANY") => ""
      case None => ""
      case Some("FLAT") => " AND is_fluctuating = false"
      case Some("INCREASING") => " AND is_fluctuating = false"
      case Some("DECREASING") => " AND is_fluctuating = false"
      case _ => ""
    }
  }

  def generateValueRangeConstraint(valueRange: SubsequenceMatchingDFA.ValueRange,
                                   epsilon: Double = 0.0): String = {
    valueRange match {
      case SubsequenceMatchingDFA.InclusiveRange(min, max) =>
        val low = min - epsilon
        val high = max + epsilon
        s" AND ST_YMax(time_series_segments) >= $low AND ST_YMin(time_series_segments) <= $high"
      case SubsequenceMatchingDFA.ExclusiveBothRange(min, max) =>
        val low = min - epsilon
        val high = max + epsilon
        s" AND ST_YMax(time_series_segments) > $low AND ST_YMin(time_series_segments) < $high"
      case SubsequenceMatchingDFA.LeftInclusiveRightExclusiveRange(min, max) =>
        val low = min - epsilon
        val high = max + epsilon
        s" AND ST_YMax(time_series_segments) >= $low AND ST_YMin(time_series_segments) < $high"
      case SubsequenceMatchingDFA.LeftExclusiveRightInclusiveRange(min, max) =>
        val low = min - epsilon
        val high = max + epsilon
        s" AND ST_YMax(time_series_segments) > $low AND ST_YMin(time_series_segments) <= $high"
      case SubsequenceMatchingDFA.GreaterThanRange(min, inclusive) =>
        val low = min - epsilon
        if (inclusive) s" AND ST_YMax(time_series_segments) >= $low"
        else s" AND ST_YMax(time_series_segments) > $low"
      case SubsequenceMatchingDFA.LessThanRange(max, inclusive) =>
        val high = max + epsilon
        if (inclusive) s" AND ST_YMin(time_series_segments) <= $high"
        else s" AND ST_YMin(time_series_segments) < $high"
    }
  }

  def directionToString(direction: SubsequenceMatchingDFA.Direction): Option[String] = {
    direction match {
      case SubsequenceMatchingDFA.Increasing => Some("INCREASING")
      case SubsequenceMatchingDFA.Decreasing => Some("DECREASING")
      case SubsequenceMatchingDFA.Flat => Some("FLAT")
      case SubsequenceMatchingDFA.Any => Some("ANY")
    }
  }


  def executeQueryOnTableAndHostMachine(
    tableIdentifier: String,
    hostMachine: String,
    query: ((Double, Double, Double, Double), String),
    databases: Map[String, String],
    threshold: Double,
    span90: Double,
    directionConstraint: Option[String] = None
  ): Future[Set[Int]] = Future {
    val ((llx, lly, urx, ury), flag) = query
    val url = databases(hostMachine)
    val props = new java.util.Properties()
    props.setProperty("user", "postgres")
    props.setProperty("password", "YOUR_PASSWORD_HERE")
    val tableName = tableIdentifier
    var conn: java.sql.Connection = null
    var stmt: java.sql.Statement = null
    var rs: java.sql.ResultSet = null
    val ids = mutable.Set[Int]()
    val t0 = System.currentTimeMillis()
    val useStreaming = threshold >= span90

    try {
      conn = DriverManager.getConnection(url, props)
      if (useStreaming) {
        stmt = conn.createStatement(
          java.sql.ResultSet.TYPE_FORWARD_ONLY,
          java.sql.ResultSet.CONCUR_READ_ONLY
        )
        stmt.setFetchSize(20000)
        logger.info(s"[$hostMachine.$tableName] Using streaming mode with fetch size 20000")
      } else {
        stmt = conn.createStatement()
      }

      val slopeConstraint = generateSlopeConstraint(directionConstraint)
      val fluctuationConstraint = generateFluctuationConstraint(directionConstraint)

      val sql = if (llx == urx || lly == ury) {
        s"""SELECT DISTINCT time_series_id
           |FROM $tableName
           |WHERE ST_Intersects(
           |  time_series_segments,
           |  ST_SetSRID(
           |    ST_MakeLine(
           |      ST_Point($llx, $lly),
           |      ST_Point($urx, $ury)
           |    ), 4326
           |  )
           |)$slopeConstraint$fluctuationConstraint""".stripMargin
      } else {
        s"""SELECT DISTINCT time_series_id
           |FROM $tableName
           |WHERE ST_Intersects(
           |  time_series_segments,
           |  ST_SetSRID(
           |    ST_MakeEnvelope($llx, $lly, $urx, $ury), 4326
           |  )
           |)$slopeConstraint$fluctuationConstraint""".stripMargin
      }

      if (slopeConstraint.nonEmpty) {
        logger.info(s"[$hostMachine.$tableName] Executing query with slope constraint: $directionConstraint")
        logger.debug(s"SQL: $sql")
      }

      rs = stmt.executeQuery(sql)
      while (rs.next()) ids += rs.getInt("time_series_id")

      val t1 = System.currentTimeMillis()
      val constraintInfo = if (slopeConstraint.nonEmpty) s" with ${directionConstraint.getOrElse("UNKNOWN")} slope constraint" else ""
      logger.info(s"[$hostMachine.$tableName] Retrieved ${ids.size} IDs in ${(t1 - t0) / 1000.0}s$constraintInfo")
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    ids.toSet
  }

  def executeSubsequenceQuery(
    tableIdentifier: String,
    hostMachine: String,
    databases: Map[String, String],
    pattern: SubsequenceMatchingDFA.Pattern,
    threshold: Double,
    span90: Double,
    epsilon: Double = 0.0
  ): Future[Set[Int]] = Future {
    val url = databases(hostMachine)
    val props = new java.util.Properties()
    props.setProperty("user", "postgres")
    props.setProperty("password", "YOUR_PASSWORD_HERE")
    val tableName = tableIdentifier
    var conn: java.sql.Connection = null
    var stmt: java.sql.Statement = null
    var rs: java.sql.ResultSet = null
    val ids = mutable.Set[Int]()
    val t0 = System.currentTimeMillis()
    val useStreaming = threshold >= span90

    try {
      conn = DriverManager.getConnection(url, props)
      if (useStreaming) {
        stmt = conn.createStatement(
          java.sql.ResultSet.TYPE_FORWARD_ONLY,
          java.sql.ResultSet.CONCUR_READ_ONLY
        )
        stmt.setFetchSize(20000)
        logger.info(s"[$hostMachine.$tableName] Using streaming mode for subsequence matching")
      } else {
        stmt = conn.createStatement()
      }

      stmt.execute("SET max_parallel_workers_per_gather = 0")

      val directionConstraint = directionToString(pattern.direction)
      val slopeConstraint = generateSlopeConstraint(directionConstraint)
      val valueRangeConstraint = generateValueRangeConstraint(pattern.valueRange, epsilon)
      val fluctuationConstraint = generateFluctuationConstraint(directionConstraint)

      val sql = s"""SELECT DISTINCT time_series_id
         |FROM $tableName
         |WHERE 1=1
         |$slopeConstraint
         |$valueRangeConstraint
         |$fluctuationConstraint""".stripMargin

      logger.info(s"[$hostMachine.$tableName] Subsequence query with direction=${directionConstraint.getOrElse("NONE")}")
      logger.debug(s"SQL: $sql")

      rs = stmt.executeQuery(sql)
      while (rs.next()) ids += rs.getInt("time_series_id")

      val t1 = System.currentTimeMillis()
      logger.info(s"[$hostMachine.$tableName] Subsequence query retrieved ${ids.size} IDs in ${(t1 - t0) / 1000.0}s")
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    ids.toSet
  }

  def executeDesignSpaceSql(
    tableIdentifier: String,
    hostMachine: String,
    databases: Map[String, String],
    sql: String,
    threshold: Double,
    span90: Double
  ): Future[Set[Int]] = Future {
    val url = databases(hostMachine)
    val props = new java.util.Properties()
    props.setProperty("user", "postgres")
    props.setProperty("password", "YOUR_PASSWORD_HERE")
    var conn: java.sql.Connection = null
    var stmt: java.sql.Statement = null
    var rs: java.sql.ResultSet = null
    val ids = mutable.Set[Int]()
    val t0 = System.currentTimeMillis()
    val useStreaming = threshold >= span90

    try {
      conn = DriverManager.getConnection(url, props)
      if (useStreaming) {
        stmt = conn.createStatement(
          java.sql.ResultSet.TYPE_FORWARD_ONLY,
          java.sql.ResultSet.CONCUR_READ_ONLY
        )
        stmt.setFetchSize(20000)
        logger.info(s"[$hostMachine.$tableIdentifier] Using streaming mode for design space query")
      } else {
        stmt = conn.createStatement()
      }

      stmt.execute("SET max_parallel_workers_per_gather = 0")

      logger.info(s"[$hostMachine.$tableIdentifier] Executing design space SQL")
      logger.debug(s"SQL: $sql")

      rs = stmt.executeQuery(sql)
      while (rs.next()) ids += rs.getInt("time_series_id")

      val t1 = System.currentTimeMillis()
      logger.info(s"[$hostMachine.$tableIdentifier] Design space query retrieved ${ids.size} IDs in ${(t1 - t0) / 1000.0}s")
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    ids.toSet
  }

  def saveIdsToCsv(tableIdentifier: String, ids: Set[Int], runIdx: Int, timestamp: String): Unit = {
    val matchingType = Config.MATCHING_TYPE.toLowerCase
    val subCase = if (Config.MATCHING_TYPE == "SUBSEQUENCE") s"_${Config.SUBSEQUENCE_CASE}" else ""
    val datasetDir = s"query_results_${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_${matchingType}${subCase}"
    val outDir = new File(s"/home/YOUR_USERNAME/results/$datasetDir/")
    if (!outDir.exists()) outDir.mkdirs()
    val path = s"/home/YOUR_USERNAME/results/$datasetDir/${tableIdentifier}_run${runIdx}_$timestamp.csv"
    val w = new BufferedWriter(new FileWriter(path))
    try {
      w.write("time_series_id\n")
      ids.foreach(id => w.write(s"$id\n"))
    } finally {
      w.close()
    }
  }


  def main(args: Array[String]): Unit = {
    val DATASET = Config.DATASET
    val DATASET_SIZE = Config.DATASET_SIZE
    val ALPHA = Config.ALPHA
    val MATCHING_TYPE = Config.MATCHING_TYPE
    val SUBSEQUENCE_CASE = Config.SUBSEQUENCE_CASE

    val appName = f"QueryProcessing_DesignSpace_${DATASET}_${DATASET_SIZE}_alpha${ALPHA}%.2f_${MATCHING_TYPE}_${SUBSEQUENCE_CASE}"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("spark://YOUR_SPARK_MASTER_HOST:7077")
      .set("spark.executor.memory", "32g")
      .set("spark.driver.memory", "64g")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memoryOverhead", "1g")
      .set("dfs.client.use.datanode.hostname", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/home/YOUR_USERNAME/spark-temp")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val datasetMappingDir = s"${DATASET.toLowerCase}_${DATASET_SIZE}_mapping"
    val mappingBasePath = s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/$datasetMappingDir"

    val s1HdfsStart = System.nanoTime()
    val gridCellCounts = readMapFromHDFS[String, Int](
      s"$mappingBasePath/gridCellAccumulator",
      spark, identity, _.toInt
    )

    val rawTableToHost = readMapFromHDFS[String, String](
      s"$mappingBasePath/tableToHostMapping",
      spark, identity, identity
    )
    val ipToHost = Map("YOUR_DB_SERVER1_IP" -> "shark1", "YOUR_DB_SERVER2_IP" -> "shark2")
    val tableToHost = rawTableToHost.map { case (fullTableName, host) =>
      fullTableName -> ipToHost.getOrElse(host, host)
    }
    val s1HdfsElapsed = (System.nanoTime() - s1HdfsStart) / 1e9d
    println(f"PHASE_TIMING stage1_hdfs_metadata=$s1HdfsElapsed%.2f")

    val databases = Map(
      "shark1" -> "jdbc:postgresql://YOUR_SPARK_MASTER_HOST:5432/postgres",
      "shark2" -> "jdbc:postgresql://YOUR_DB_SERVER2_HOST:5432/postgres"
    )

    val params = Config.getParams()
    val threshold = params.threshold
    val span90 = params.span90
    val gridW = params.gridW
    val gridH = params.gridH
    val yMin = params.yMin
    val yMax = params.yMax
    val epsilon = params.epsilon

    println(s"=== DATASET: $DATASET, SIZE: $DATASET_SIZE, MATCHING TYPE: $MATCHING_TYPE ===")
    if (MATCHING_TYPE == "SUBSEQUENCE") println(s"=== SUBSEQUENCE CASE: $SUBSEQUENCE_CASE ===")
    println(s"Threshold: $threshold, Span90%: $span90, Epsilon: $epsilon")
    println(s"Grid parameters: w=$gridW, h=$gridH, y-range=[$yMin, $yMax]")

    val queryPattern = Config.getPattern(DATASET, MATCHING_TYPE)
    println(s"Pattern: $queryPattern")

    val gridCells = generateGrid(gridW, gridH, yMin, yMax)

    val cbdGridCells = gridCells.map(c =>
      CostBasedQueryDecision.GridCell(c.grid_id, c.xBL, c.yBL, c.xTR, c.yTR)
    ).toList

    val numRuns = 1
    val durations = ArrayBuffer[Double]()

    val batchSize = if (tableToHost.size > 160) 8
                   else if (tableToHost.size > 80)  16
                   else if (tableToHost.size > 40)  20
                   else 20


    if (MATCHING_TYPE == "SUBSEQUENCE") {
      println(s"=== SUBSEQUENCE MATCHING MODE — Case $SUBSEQUENCE_CASE ===")

      val results = mutable.Map[String, Set[Int]]()

      SUBSEQUENCE_CASE match {
        case "FIXED_START_FIXED_LEN" | "FIXED_START_DYN_LEN"
           | "BOUNDED_FIXED_LEN" | "BOUNDED_DYN_LEN" =>
          val subQuery = SubsequenceQueryParser.parse(queryPattern, epsilon)
          println(s"Detected case: ${subQuery.detectedCase}")
          println(s"Patterns: ${subQuery.patterns.size}, Probes: ${subQuery.probes.size}")

          for (runNum <- 1 to numRuns) {
            println(s"=== RUN #${runNum}/1: Case $SUBSEQUENCE_CASE ===")
            val t0 = System.currentTimeMillis()

            val costDecisionStartTime = System.currentTimeMillis()

            val (useSpatial, bestProbeIdx) = subQuery.detectedCase match {
              case SubsequenceQueryParser.Case1A | SubsequenceQueryParser.Case1B
                 | SubsequenceQueryParser.CaseFixedStartFixedLen
                 | SubsequenceQueryParser.CaseFixedStartDynLen
                 | SubsequenceQueryParser.CaseBoundedFixedLen
                 | SubsequenceQueryParser.CaseBoundedDynLen =>
                val probes = subQuery.probes.head
                val spatialCost = CostBasedQueryDecision.estimateSpatialIntersectCost(
                  gridCellCounts, cbdGridCells, probes)
                val btreeCost = CostBasedQueryDecision.estimateBtreeCost(
                  gridCellCounts.values.sum, probes.pattern.direction,
                  probes.pattern.yMin, probes.pattern.yMax, yMin, yMax)
                val decision = spatialCost <= btreeCost
                println(s"Cost estimate: spatial=$spatialCost, btree=$btreeCost → ${if (decision) "SPATIAL" else "B-TREE"}")
                (decision, 0)

              case SubsequenceQueryParser.Case2A =>
                val configStrategy = Config.MULTI_PATTERN_STRATEGY
                println(s"Multi-pattern strategy (from config): $configStrategy")

                configStrategy match {
                  case "INTERSECT_ALL" =>
                    println(s"INTERSECT-ALL: spatial queries for ALL ${subQuery.probes.size} patterns")
                    (true, -1)

                  case "BEST_ONLY" =>
                    val bestIdx = SubsequenceQueryParser.selectMostSelectivePattern(subQuery.patterns)
                    val bestProbes = subQuery.probes(bestIdx)
                    val spatialCost = CostBasedQueryDecision.estimateSpatialIntersectCost(
                      gridCellCounts, cbdGridCells, bestProbes)
                    val btreeCost = CostBasedQueryDecision.estimateBtreeCost(
                      gridCellCounts.values.sum, bestProbes.pattern.direction,
                      bestProbes.pattern.yMin, bestProbes.pattern.yMax, yMin, yMax)
                    val decision = spatialCost <= btreeCost
                    println(s"BEST-ONLY: pattern index $bestIdx, spatial=$spatialCost, btree=$btreeCost → ${if (decision) "SPATIAL" else "B-TREE"}")
                    (decision, bestIdx)

                  case "CASCADE" =>
                    println(s"CASCADE: will process patterns sequentially")
                    (true, -2)

                  case _ =>
                    throw new IllegalArgumentException(s"Unknown MULTI_PATTERN_STRATEGY: $configStrategy")
                }

              case _ => (false, 0)
            }

            val costDecisionEndTime = System.currentTimeMillis()
            println(s"Cost-based decision took ${(costDecisionEndTime - costDecisionStartTime) / 1000.0}s")

            val queryDistributionStartTime = System.currentTimeMillis()
            println(s"Processing ${tableToHost.size} tables in batches of $batchSize")
            val tableList = tableToHost.toList
            results.clear()

            if (bestProbeIdx == -2) {
              val rankedIndices = SubsequenceQueryParser.rankPatternsBySelectivity(subQuery.patterns)
              val cascadeBatchSize = 50000
              println(s"CASCADE: processing patterns in selectivity order: ${rankedIndices.mkString(" → ")}")

              for ((table, host) <- tableList) {
                var candidateIds: Set[Int] = null

                for (rank <- rankedIndices.indices) {
                  val patIdx = rankedIndices(rank)

                  if (candidateIds == null) {
                    val sql = SubsequenceQueryParser.generateSpatialIntersectSql(table, subQuery.probes(patIdx))
                    val ids = Await.result(
                      executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                      Duration.Inf)
                    candidateIds = ids
                    println(s"[$table] Pattern $patIdx (first): ${candidateIds.size} candidates")
                  } else if (candidateIds.nonEmpty) {
                    val candidateArray = candidateIds.toArray.map(_.toLong)
                    val newCandidates = mutable.Set[Int]()

                    for (chunk <- candidateArray.grouped(cascadeBatchSize)) {
                      val sql = SubsequenceQueryParser.generateCascadeNextSpatialSql(
                        table, subQuery.probes(patIdx), chunk)
                      val chunkIds = Await.result(
                        executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                        Duration.Inf)
                      newCandidates ++= chunkIds
                    }

                    candidateIds = newCandidates.toSet
                    println(s"[$table] Pattern $patIdx (cascade): ${candidateIds.size} candidates remaining")
                  }
                }

                val finalIds = if (candidateIds != null) candidateIds else Set.empty[Int]
                val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                saveIdsToCsv(table, finalIds, runNum, ts)
                results += table -> finalIds
              }

            } else {
              for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                val futures = if (useSpatial && bestProbeIdx == -1) {
                  for ((table, host) <- batch) yield {
                    val sql = SubsequenceQueryParser.generateMultiPatternSpatialIntersectSql(table, subQuery.probes)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                } else if (useSpatial) {
                  val chosenProbes = subQuery.probes(bestProbeIdx)
                  for ((table, host) <- batch) yield {
                    val sql = SubsequenceQueryParser.generateSpatialIntersectSql(table, chosenProbes)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                } else {
                  val pattern = if (bestProbeIdx >= 0) subQuery.patterns(bestProbeIdx) else subQuery.patterns.head
                  for ((table, host) <- batch) yield {
                    val sql = SubsequenceQueryParser.generateBtreeSql(table, pattern, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                }

                val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                  .groupBy(_._1)
                  .map { case (table, lists) =>
                    table -> lists.flatMap(_._2).toSet
                  }

                val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                batchResults.foreach { case (table, ids) =>
                  saveIdsToCsv(table, ids, runNum, ts)
                }

                results ++= batchResults.map { case (table, ids) =>
                  table -> ids
                }

                println(s"Batch ${batchIdx + 1}/${Math.ceil(tableList.size.toDouble / batchSize).toInt} completed: ${results.size}/${tableToHost.size} tables processed")
                System.gc()
                Thread.sleep(500)
              }
            }

            val queryDistributionEndTime = System.currentTimeMillis()
            println(s"Query distribution and execution took ${(queryDistributionEndTime - queryDistributionStartTime) / 1000.0}s")

            val t1 = System.currentTimeMillis()
            val runSec = (t1 - t0) / 1000.0
            println(f"Case $SUBSEQUENCE_CASE run took $runSec%.2f seconds")
            durations += runSec
          }

        case "FREE_FIXED_LEN" | "FREE_DYN_LEN" | "FREE_DYN_LEN_WIDE" =>
          val (pattern, thresholdObj) = SubsequenceMatchingDFA.parseQuery(queryPattern)
          println(s"Parsed pattern: direction=${pattern.direction}, valueRange=${pattern.valueRange}, length=[${pattern.minLength},${pattern.maxLength}]")

          for (runNum <- 1 to numRuns) {
            println(s"=== RUN #${runNum}/1: Case $SUBSEQUENCE_CASE (B-tree only) ===")
            val t0 = System.currentTimeMillis()

            println(s"Processing ${tableToHost.size} tables in batches of $batchSize with SAFE filters (no spatial query)")
            val tableList = tableToHost.toList
            results.clear()

            for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
              val futures = for {
                (table, host) <- batch
              } yield {
                executeSubsequenceQuery(table, host, databases, pattern, threshold, span90, epsilon)
                  .map(ids => (table, ids))
              }

              val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                .groupBy(_._1)
                .map { case (table, lists) =>
                  table -> lists.flatMap(_._2).toSet
                }

              val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
              batchResults.foreach { case (table, ids) =>
                saveIdsToCsv(table, ids, runNum, ts)
              }

              results ++= batchResults.map { case (table, ids) =>
                table -> ids
              }

              println(s"Batch ${batchIdx + 1}/${Math.ceil(tableList.size.toDouble / batchSize).toInt} completed: ${results.size}/${tableToHost.size} tables processed")
              System.gc()
              Thread.sleep(500)
            }

            println(s"All ${tableToHost.size} tables processed for Case 1C")

            val t1 = System.currentTimeMillis()
            val runSec = (t1 - t0) / 1000.0
            println(f"Case $SUBSEQUENCE_CASE run took $runSec%.2f seconds")
            durations += runSec
          }

        case "MULTI_ADJACENT" | "MULTI_FIXED_GAP" | "MULTI_RANGE_GAP"
           | "MULTI_ORDERED_FLEX_GAP" | "MULTI_UNORDERED_FLEX_GAP" =>
          val subQuery = SubsequenceQueryParser.parse(queryPattern, epsilon)
          val strategy = Config.MULTI_PATTERN_STRATEGY
          println(s"Detected case: ${subQuery.detectedCase}")
          println(s"Patterns: ${subQuery.patterns.size}")
          println(s"Multi-pattern strategy: $strategy")
          if (subQuery.gaps.nonEmpty) {
            val gapStr = subQuery.gaps.map { g =>
              if (g.isFixed) s"#${g.minLength}#" else s"#${g.minLength}-${g.maxLength}#"
            }.mkString(", ")
            println(s"Gaps: $gapStr")
          }
          if (subQuery.unordered) println(s"UNORDERED: patterns may appear in any order")

          val rankedIndices = SubsequenceQueryParser.rankPatternsBySelectivity(subQuery.patterns)
          println(s"Pattern selectivity ranking: ${rankedIndices.mkString(" → ")}")

          for (runNum <- 1 to numRuns) {
            println(s"=== RUN #${runNum}/1: Case $SUBSEQUENCE_CASE ($strategy) ===")
            val t0 = System.currentTimeMillis()

            val tableList = tableToHost.toList
            results.clear()

            strategy match {
              case "INTERSECT_ALL" =>
                println(s"Processing ${tableToHost.size} tables with multi-pattern B-tree INTERSECT")
                for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                  val futures = for ((table, host) <- batch) yield {
                    val sql = SubsequenceQueryParser.generateMultiPatternBtreeIntersectSql(
                      table, subQuery.patterns, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }

                  val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                    .groupBy(_._1).map { case (table, lists) => table -> lists.flatMap(_._2).toSet }

                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  batchResults.foreach { case (table, ids) => saveIdsToCsv(table, ids, runNum, ts) }
                  results ++= batchResults.map { case (table, ids) => table -> ids }

                  println(s"Batch ${batchIdx + 1} completed: ${results.size}/${tableToHost.size} tables")
                  System.gc(); Thread.sleep(500)
                }

              case "COMPOSITE" =>
                println(s"Processing ${tableToHost.size} tables with multi-pattern COMPOSITE (single-scan GROUP BY + HAVING)")
                for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                  val futures = for ((table, host) <- batch) yield {
                    val sql = SubsequenceQueryParser.generateMultiPatternCompositeSql(
                      table, subQuery.patterns, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }

                  val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                    .groupBy(_._1).map { case (table, lists) => table -> lists.flatMap(_._2).toSet }

                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  batchResults.foreach { case (table, ids) => saveIdsToCsv(table, ids, runNum, ts) }
                  results ++= batchResults.map { case (table, ids) => table -> ids }

                  println(s"Batch ${batchIdx + 1} completed: ${results.size}/${tableToHost.size} tables")
                  System.gc(); Thread.sleep(500)
                }

              case "BEST_ONLY" =>
                val bestIdx = SubsequenceQueryParser.selectMostSelectivePattern(subQuery.patterns)
                println(s"BEST_ONLY: using pattern index $bestIdx (most selective)")
                for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                  val futures = for ((table, host) <- batch) yield {
                    val sql = SubsequenceQueryParser.generateBestOnlyBtreeSql(
                      table, subQuery.patterns, bestIdx, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }

                  val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                    .groupBy(_._1).map { case (table, lists) => table -> lists.flatMap(_._2).toSet }

                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  batchResults.foreach { case (table, ids) => saveIdsToCsv(table, ids, runNum, ts) }
                  results ++= batchResults.map { case (table, ids) => table -> ids }

                  println(s"Batch ${batchIdx + 1} completed: ${results.size}/${tableToHost.size} tables")
                  System.gc(); Thread.sleep(500)
                }

              case "CASCADE" =>
                println(s"CASCADE: processing patterns in selectivity order: ${rankedIndices.mkString(" → ")}")
                val cascadeBatchSize = 50000

                for ((table, host) <- tableList) {
                  var candidateIds: Set[Int] = null

                  for (rank <- rankedIndices.indices) {
                    val patIdx = rankedIndices(rank)
                    val pattern = subQuery.patterns(patIdx)

                    if (candidateIds == null) {
                      val sql = SubsequenceQueryParser.generateCascadeFirstBtreeSql(table, pattern, epsilon)
                      val ids = Await.result(
                        executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                        Duration.Inf)
                      candidateIds = ids
                      println(s"[$table] Pattern $patIdx (first): ${candidateIds.size} candidates")
                    } else if (candidateIds.nonEmpty) {
                      val candidateArray = candidateIds.toArray.map(_.toLong)
                      val newCandidates = mutable.Set[Int]()

                      for (chunk <- candidateArray.grouped(cascadeBatchSize)) {
                        val sql = SubsequenceQueryParser.generateCascadeNextBtreeSql(
                          table, pattern, epsilon, chunk)
                        val chunkIds = Await.result(
                          executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                          Duration.Inf)
                        newCandidates ++= chunkIds
                      }

                      candidateIds = newCandidates.toSet
                      println(s"[$table] Pattern $patIdx (cascade): ${candidateIds.size} candidates remaining")
                    }
                  }

                  val finalIds = if (candidateIds != null) candidateIds else Set.empty[Int]
                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  saveIdsToCsv(table, finalIds, runNum, ts)
                  results += table -> finalIds

                  if (results.size % batchSize == 0) {
                    println(s"Progress: ${results.size}/${tableToHost.size} tables")
                    System.gc()
                  }
                }

              case _ =>
                throw new IllegalArgumentException(s"Unknown MULTI_PATTERN_STRATEGY: $strategy")
            }

            println(s"All ${tableToHost.size} tables processed for Case $SUBSEQUENCE_CASE ($strategy)")

            val t1 = System.currentTimeMillis()
            val runSec = (t1 - t0) / 1000.0
            println(f"Case $SUBSEQUENCE_CASE run took $runSec%.2f seconds")
            durations += runSec
          }

        case _ =>
          throw new IllegalArgumentException(s"Unknown SUBSEQUENCE_CASE: $SUBSEQUENCE_CASE")
      }

      val s1SqlElapsed = durations.sum
      println(f"PHASE_TIMING stage1_sql=$s1SqlElapsed%.2f")
      println(f"PHASE_TIMING stage1_total=${s1HdfsElapsed + s1SqlElapsed}%.2f")

      val refinementOutputBase = DATASET match {
        case "TSBS"       => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/tsbs"
        case "ECG"        => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/ecg"
        case "RANDOMWALK" => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/random_walk"
        case _            => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/${DATASET.toLowerCase}"
      }

      val groundTruthSeeds: Set[Long] = Config.getGroundTruthSeeds(DATASET)

      println("\n" + "=" * 80)
      println("=== FUSED STAGE 2 STARTING (same SparkContext as Stage 1) ===")
      println("=" * 80)
      val fusedStage2Start = System.currentTimeMillis()
      SubsequenceRetrievalRefinement.runSubsequenceRefinementInline(
        spark,
        results.toMap,
        queryPattern,
        refinementOutputBase,
        groundTruthSeeds
      )
      val fusedStage2End = System.currentTimeMillis()
      val fusedStage2Wall = (fusedStage2End - fusedStage2Start) / 1000.0
      println(f"FUSED STAGE 2 wall: $fusedStage2Wall%.2f seconds")
      println(f"PHASE_TIMING stage2_wall=$fusedStage2Wall%.2f")

    } else if (MATCHING_TYPE == "WHOLE_SEQUENCE") {
      println("=== WHOLE-SEQUENCE MATCHING MODE (Enhanced with both-probe INTERSECT) ===")

      val patterns = Seq(queryPattern)
      val query1Pattern = patterns(0)

      for (runNum <- 1 to numRuns) {
        println(s"=== RUN #${runNum}/1: pattern = $query1Pattern")
        val t0 = System.currentTimeMillis()

        val queryParsingStartTime = System.currentTimeMillis()
        val windowQueries = QueryParser.parsePattern(query1Pattern)
        val queryParsingEndTime = System.currentTimeMillis()
        println(s"Enhanced window query parsing took ${(queryParsingEndTime - queryParsingStartTime) / 1000.0} seconds")

        val querySelectionStartTime = System.currentTimeMillis()
        var chosen: Option[((Double, Double, Double, Double), (Double, Double, Double, Double), String, Option[QueryParser.SubpatternConstraints])] = None
        var bestCount = Int.MaxValue

        println("=== Enhanced Window Query Intersection Counts ===")
        for ((ls, rs, flag, constraints) <- windowQueries) {
          val isDynamicSubpattern = rs == (0.0, 0.0, 0.0, 0.0)

          val count = if (flag.startsWith("|") && flag.endsWith("|")) {
            if (isDynamicSubpattern) {
              val maxLength = constraints.flatMap(_.lengthRange).map(_._2).getOrElse(128)
              val rightX = ls._1 + maxLength - 1
              val rightWindow = (rightX, ls._2, rightX, ls._4)

              val leftCells = gridCells.filter(isIntersecting(ls, _))
              val rightCells = gridCells.filter(isIntersecting(rightWindow, _))
              val leftCount = leftCells.map(c => gridCellCounts.getOrElse(c.grid_id, 0)).sum
              val rightCount = rightCells.map(c => gridCellCounts.getOrElse(c.grid_id, 0)).sum

              math.min(leftCount, rightCount)
            } else {
              val leftCells = gridCells.filter(isIntersecting(ls, _))
              val rightCells = gridCells.filter(isIntersecting(rs, _))
              val leftCount = leftCells.map(c => gridCellCounts.getOrElse(c.grid_id, 0)).sum
              val rightCount = rightCells.map(c => gridCellCounts.getOrElse(c.grid_id, 0)).sum
              math.min(leftCount, rightCount)
            }
          } else {
            gridCells.filter(isIntersecting(ls, _))
              .map(c => gridCellCounts.getOrElse(c.grid_id, 0)).sum
          }

          if (flag.startsWith("|") && flag.endsWith("|")) {
            val directionInfo = constraints.flatMap(_.direction).getOrElse("NONE")
            val dynInfo = if (isDynamicSubpattern) " [DYNAMIC - both probes]" else " [FIXED - both probes]"
            println(s"!!!Subpattern Window Query: Direction: $directionInfo, INTERSECT Est Count: $count$dynInfo")
          } else {
            println(s"!!!Window Query: $ls, Total Intersected Count: $count")
          }

          if (count > 0 && count < bestCount) {
            bestCount = count
            chosen = Some((ls, rs, flag, constraints))
          }
        }
        println("=========================================")

        val querySelectionEndTime = System.currentTimeMillis()
        println(s"!!!!Enhanced Window Query Selection Time: ${(querySelectionEndTime - querySelectionStartTime) / 1000.0} seconds.")

        chosen match {
          case None =>
            println("No matching segments found; skipping.")
          case Some((ls, rs, flag, constraints)) =>
            val directionInfo = constraints.flatMap(_.direction).getOrElse("NONE")
            println(s"Chosen query: window with direction constraint $directionInfo, count=$bestCount")

            val queryDistributionStartTime = System.currentTimeMillis()

            val directionConstraint = constraints.flatMap(_.direction)
            val isDynamicSubpattern = rs == (0.0, 0.0, 0.0, 0.0)

            val toRun = if (flag.startsWith("|") && flag.endsWith("|")) {
              if (isDynamicSubpattern) {
                val maxLength = constraints.flatMap(_.lengthRange).map(_._2).getOrElse(128)
                val rightX = ls._1 + maxLength - 1
                val rightWindow = (rightX, ls._2, rightX, ls._4)
                println(s"Dynamic subpattern: left probe at x=${ls._1}, right probe at x=$rightX (maxLen=$maxLength)")
                List((ls, flag, directionConstraint), (rightWindow, flag, directionConstraint))
              } else {
                List((ls, flag, directionConstraint), (rs, flag, directionConstraint))
              }
            } else {
              List((ls, flag, None))
            }

            println(s"Distributing ${toRun.size} probe(s) with INTERSECT across ${tableToHost.size} tables in batches of $batchSize")
            val tableList = tableToHost.toList
            val results = mutable.Map[String, Set[Int]]()

            for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
              val futures = for {
                ((seg, fl, direction), probeIdx) <- toRun.zipWithIndex
                (table, host) <- batch
              } yield {
                executeQueryOnTableAndHostMachine(table, host, (seg, fl), databases, threshold, span90, direction)
                  .map(ids => (table, probeIdx, ids))
              }

              val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                .groupBy(_._1)
                .map { case (table, probeResults) =>
                  if (toRun.size > 1) {
                    val probeGroups = probeResults.groupBy(_._2)
                    table -> probeGroups.values.map(_.flatMap(_._3).toSet).reduce(_ intersect _)
                  } else {
                    table -> probeResults.flatMap(_._3).toSet
                  }
                }

              val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
              batchResults.foreach { case (table, ids) =>
                saveIdsToCsv(table, ids, runNum, ts)
              }

              results ++= batchResults.map { case (table, ids) =>
                table -> ids
              }

              println(s"Batch ${batchIdx + 1}/${Math.ceil(tableList.size.toDouble / batchSize).toInt} completed: ${results.size}/${tableToHost.size} tables processed")
              System.gc()
              Thread.sleep(500)
            }

            println(s"All ${tableToHost.size} tables processed with INTERSECT")

            val queryDistributionEndTime = System.currentTimeMillis()
            println(s"Enhanced Query Distribution and Execution Time: ${(queryDistributionEndTime - queryDistributionStartTime) / 1000.0} seconds")
        }

        val t1 = System.currentTimeMillis()
        val runSec = (t1 - t0) / 1000.0
        println(f"Whole-sequence matching run took $runSec%.2f seconds")
        durations += runSec
      }
    } else {
      throw new IllegalArgumentException(s"Unknown MATCHING_TYPE: $MATCHING_TYPE")
    }

    val total = durations.sum
    val avgTime = total / numRuns
    val minTime = durations.min
    val maxTime = durations.max
    println(f"All $numRuns runs finished in ${total}%.2f seconds")
    println(f"Average time per run: ${avgTime}%.3f seconds")
    println(f"Min/Max time: ${minTime}%.3f / ${maxTime}%.3f seconds")

    spark.stop()
  }
}
