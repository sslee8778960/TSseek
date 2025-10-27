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

object QueryProcessing {

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

  /** Reads a simple key,value map from HDFS text file. */
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

  /**
   * Generate slope constraint SQL based on direction
   */
  def generateSlopeConstraint(direction: Option[String]): String = {
    direction match {
      case Some("INCREASING") => " AND segment_slope > 0"
      case Some("DECREASING") => " AND segment_slope < 0"
      case Some("FLAT") => " AND segment_slope = 0"  // Exactly zero slope (constant values)
      case Some("ANY") | None => ""  // No slope constraint
      case _ => ""  // Unknown direction, no constraint
    }
  }

  /** Generates fluctuation constraint for PostgreSQL WHERE clause */
  def generateFluctuationConstraint(directionConstraint: Option[String]): String = {
    directionConstraint match {
      case Some("ANY") => ""  // <~> pattern: no fluctuation filtering (any direction acceptable)
      case None => ""         // Unspecified direction: no fluctuation filtering
      case Some("FLAT") => " AND is_fluctuating = false"      // <=> REQUIRES exact constancy
      case Some("INCREASING") => " AND is_fluctuating = false" // <+> requires consistent increase
      case Some("DECREASING") => " AND is_fluctuating = false" // <-> requires consistent decrease
      case _ => ""  // Unknown direction, no constraint
    }
  }

  
  def executeQueryOnTableAndHostMachine(
                                        tableIdentifier: String,
                                        hostMachine: String,
                                        query: ((Double, Double, Double, Double), String),
                                        databases: Map[String, String],
                                        threshold: Double,  // Dataset-specific threshold
                                        span90: Double,     // Dataset-specific Span90% for streaming mode decision
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

    val useStreaming = threshold >= span90  // Streaming when α ≥ 1.0 (threshold ≥ Span90%)

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
        s"""
           |SELECT DISTINCT time_series_id
           |FROM $tableName
           |WHERE ST_Intersects(
           |  time_series_segments,
           |  ST_SetSRID(
           |    ST_MakeLine(
           |      ST_Point($llx, $lly),
           |      ST_Point($urx, $ury)
           |    ), 4326
           |  )
           |)$slopeConstraint$fluctuationConstraint
           |""".stripMargin
      } else {
        s"""
           |SELECT DISTINCT time_series_id
           |FROM $tableName
           |WHERE ST_Intersects(
           |  time_series_segments,
           |  ST_SetSRID(
           |    ST_MakeEnvelope($llx, $lly, $urx, $ury), 4326
           |  )
           |)$slopeConstraint$fluctuationConstraint
           |""".stripMargin
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

  
  def saveIdsToCsv(tableIdentifier: String, ids: Set[Int], runIdx: Int, timestamp: String): Unit = {
    val datasetDir = s"query_results_${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}"
    val outDir = new File(s"/home/xli3/results/$datasetDir/")
    if (!outDir.exists()) outDir.mkdirs()
    val path = s"/home/xli3/results/$datasetDir/${tableIdentifier}_run${runIdx}_$timestamp.csv"
    val w = new BufferedWriter(new FileWriter(path))
    try {
      w.write("time_series_id\n")
      ids.foreach(id => w.write(s"$id\n"))
    } finally {
      w.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val DATASET_EARLY = Config.DATASET
    val DATASET_SIZE_EARLY = Config.DATASET_SIZE
    val ALPHA_EARLY = Config.ALPHA

    val appName = f"QueryProcessing_${DATASET_EARLY}_${DATASET_SIZE_EARLY}_alpha${ALPHA_EARLY}%.2f_Batch"

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("spark://YOUR_SPARK_MASTER_HOST:7077")
      .set("spark.executor.memory", "32g")
      .set("spark.driver.memory", "64g")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memoryOverhead", "1g")
      .set("dfs.client.use.datanode.hostname", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/home/xli3/spark-temp")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val datasetMappingDir = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_mapping"
    val mappingBasePath = s"hdfs://shark1local:9000/user/xli3/output/$datasetMappingDir"

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

    val databases = Map(
      "shark1" -> "jdbc:postgresql://YOUR_SPARK_MASTER_HOST:5432/postgres",
      "shark2" -> "jdbc:postgresql://YOUR_DB_SERVER2_HOST:5432/postgres"
    )

    val DATASET = Config.DATASET
    val DATASET_SIZE = Config.DATASET_SIZE
    val ALPHA = Config.ALPHA
    val params = Config.getParams()

    val threshold = params.threshold
    val span90 = params.span90
    val gridW = params.gridW
    val gridH = params.gridH
    val yMin = params.yMin
    val yMax = params.yMax
    val queryPattern = params.queryPattern

    println(s"=== DATASET: $DATASET ===")
    println(s"Threshold: $threshold, Span90%: $span90")
    println(s"Grid parameters: w=$gridW, h=$gridH, y-range=[$yMin, $yMax]")

    val gridCells = generateGrid(gridW, gridH, yMin, yMax)

    val patterns = Seq(queryPattern)

    val numRuns = 1
    val query1Pattern = patterns(0)

    val durations = ArrayBuffer[Double]()

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

        val cells =
          if (flag.startsWith("|") && flag.endsWith("|")) {  // subpattern check
            if (isDynamicSubpattern) {
              gridCells.filter(isIntersecting(ls, _))
            } else {
              (gridCells.filter(isIntersecting(ls, _)) ++ gridCells.filter(isIntersecting(rs, _))).distinct
            }
          } else {
            gridCells.filter(isIntersecting(ls, _))
          }

        val count = cells.map(c => gridCellCounts.getOrElse(c.grid_id, 0)).sum

        if (flag.startsWith("|") && flag.endsWith("|")) {
          val directionInfo = constraints.flatMap(_.direction).getOrElse("NONE")
          println(s"!!!Subpattern Window Query: Combined Segments, Direction: $directionInfo, Total Intersected Count: $count")
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
      val querySelectionTime = (querySelectionEndTime - querySelectionStartTime) / 1000.0
      println(s"!!!!Enhanced Window Query Selection Time: ${querySelectionTime} seconds.")

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
              List((ls, flag, directionConstraint))
            } else {
              List((ls, flag, directionConstraint), (rs, flag, directionConstraint))
            }
          } else {
            List((ls, flag, None))
          }

          val batchSize = if (tableToHost.size > 160) 5       // For 200M series
                         else if (tableToHost.size > 80) 8    // For 100M series
                         else if (tableToHost.size > 40) 10   // For 50M series
                         else 10                              // For 25M series

          println(s"Processing ${tableToHost.size} tables in batches of $batchSize with slope-based filtering")
          val tableList = tableToHost.toList
          val results = mutable.Map[String, Set[Int]]()

          for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
            val futures = for {
              (seg, fl, direction) <- toRun
              (table, host) <- batch
            } yield {
              executeQueryOnTableAndHostMachine(table, host, (seg, fl), databases, threshold, span90, direction)
                .map(ids => (table, ids))
            }

            val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
              .groupBy(_._1)
              .map { case (table, lists) =>
                table -> lists.flatMap(_._2).toSet
              }

            val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
            val runNo = runNum
            batchResults.foreach { case (table, ids) =>
              saveIdsToCsv(table, ids, runNo, ts)
            }

            results ++= batchResults.map { case (table, ids) =>
              table -> Set.empty[Int]
            }

            println(s"Batch ${batchIdx + 1}/${Math.ceil(tableList.size.toDouble / batchSize).toInt} completed: ${results.size}/${tableToHost.size} tables processed")

            System.gc()
            Thread.sleep(500)
          }

          println(s"All ${tableToHost.size} tables processed with slope constraints")

          val runtime = Runtime.getRuntime
          val totalMemory = runtime.totalMemory() / (1024 * 1024)  // MB
          val freeMemory = runtime.freeMemory() / (1024 * 1024)    // MB
          val usedMemory = totalMemory - freeMemory
          println(s"Memory usage: ${usedMemory}MB used / ${totalMemory}MB total")

          val queryDistributionEndTime = System.currentTimeMillis()
          val queryDistributionAndExecutionTime = (queryDistributionEndTime - queryDistributionStartTime) / 1000.0
          println(s"Enhanced Query Distribution and Execution Time: $queryDistributionAndExecutionTime seconds")
      }

      val t1 = System.currentTimeMillis()
      val runSec = (t1 - t0) / 1000.0
      println(f"Single Query 1 run took $runSec%.2f seconds")
      durations += runSec
    }

    val total = durations.sum
    val avgTime = total / numRuns
    val minTime = durations.min
    val maxTime = durations.max
    println(f"All $numRuns enhanced runs finished in ${total}%.2f seconds")
    println(f"Average time per run: ${avgTime}%.3f seconds")
    println(f"Min/Max time: ${minTime}%.3f / ${maxTime}%.3f seconds")
    println(f"Total throughput: ${numRuns * 25000000 / total / 1000000}%.2f M time series processed")

    spark.stop()
  }
}