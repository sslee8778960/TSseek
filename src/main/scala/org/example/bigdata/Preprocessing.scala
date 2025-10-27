package org.example.bigdata

import sys.process._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{Partitioner, SparkConf, TaskContext}
import org.apache.spark.SparkEnv
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger

import java.io._
import java.net.InetAddress
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date
import java.io.File
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.math._
import scala.sys.process._
import scala.collection.mutable.ListBuffer
import org.apache.spark.input.PortableDataStream
import java.net.InetAddress
import scala.util.Try

object Preprocessing {

  private val logger = LoggerFactory.getLogger(Preprocessing.getClass)

  case class Point(x: Double, y: Double)

  case class Segment(startPoint: Point, endPoint: Point, slope: Double)

  case class GridCell(xBL: Double, yBL: Double, xTR: Double, yTR: Double, machine: String) {
    def grid_id: String = s"${xBL}_${yBL}_${xTR}_${yTR}"
  }

  case class GridCellSegments(cell: GridCell, segments: List[(Int, (Point, Point))])

  case class GridIndex(gridCells: Array[GridCell]) {
    private val cellWidth = if (gridCells.nonEmpty) gridCells(0).xTR - gridCells(0).xBL else 1
    private val cellHeight = if (gridCells.nonEmpty) gridCells(0).yTR - gridCells(0).yBL else 1

    logger.info(s"GridIndex initialized with ${gridCells.length} cells, cellWidth=$cellWidth, cellHeight=$cellHeight")

    def getIntersectingCells(segment: (Point, Point), threshold: Double): List[GridCell] = {
      val minX = Math.min(segment._1.x, segment._2.x) - cellWidth * 0.5
      val maxX = Math.max(segment._1.x, segment._2.x) + cellWidth * 0.5
      val minY = Math.min(segment._1.y, segment._2.y) - cellHeight * 0.5
      val maxY = Math.max(segment._1.y, segment._2.y) + cellHeight * 0.5

      gridCells.filter { grid =>
        !(maxX < grid.xBL || minX > grid.xTR || maxY < grid.yBL || minY > grid.yTR)
      }.toList
    }

    def getDetailedIntersectingCells(segment: (Point, Point), threshold: Double): List[GridCell] = {
      val candidates = getIntersectingCells(segment, threshold)
      candidates.filter { grid =>
        val intersectsY = (grid.yBL <= Math.max(segment._1.y, segment._2.y) + threshold) &&
                         (grid.yTR >= Math.min(segment._1.y, segment._2.y) - threshold)
        val intersectsX = (grid.xBL <= Math.max(segment._1.x, segment._2.x) + threshold) &&
                         (grid.xTR >= Math.min(segment._1.x, segment._2.x) - threshold)
        val containsEndPoint1 = grid.xBL <= segment._1.x && segment._1.x <= grid.xTR &&
          grid.yBL <= segment._1.y && segment._1.y <= grid.yTR
        val containsEndPoint2 = grid.xBL <= segment._2.x && segment._2.x <= grid.xTR &&
          grid.yBL <= segment._2.y && segment._2.y <= grid.yTR
        (intersectsY && intersectsX) || containsEndPoint1 || containsEndPoint2
      }
    }
  }

  def calculateDistance(point: Point, intersection: Point): Double = {
    sqrt(pow(point.x - intersection.x, 2) + pow(point.y - intersection.y, 2))
  }

  def calculateSlope(segment: (Point, Point)): Double = {
    if (segment._2.x != segment._1.x) {
      BigDecimal((segment._2.y - segment._1.y) / (segment._2.x - segment._1.x))
        .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else {
      Double.PositiveInfinity
    }
  }

  def getIntersection(point: Point, segment: (Point, Point)): Point = {
    val slopeSegment = (segment._2.y - segment._1.y) / (segment._2.x - segment._1.x)
    val slopePerpendicular = -1 / slopeSegment
    val perpB = point.y - slopePerpendicular * point.x
    val segmentB = segment._1.y - slopeSegment * segment._1.x

    val x = (segmentB - perpB) / (slopePerpendicular - slopeSegment)
    val y = slopePerpendicular * x + perpB
    Point(x, y)
  }

  def newYCoordinateOnSegment(point: Point, segment: (Point, Point)): Double = {
    if (segment._1.x == segment._2.x) {
      segment._1.y
    } else {
      val slopeSegment = (segment._2.y - segment._1.y) / (segment._2.x - segment._1.x)
      val segmentB = segment._1.y - slopeSegment * segment._1.x
      slopeSegment * point.x + segmentB
    }
  }

  def generateSegmentsList(points: List[Point], threshold: Double, maxDistanceAccumulator: MaxDistanceAccumulator): List[(Point, Point, Double, Int, Boolean)] = {
    var segments = List[(Point, Point, Double, Int, Boolean)]()
    if (points.length < 2) return segments

    var currentSegment = (points.head, points(1), 0)
    maxDistanceAccumulator.add((0, 0.0))
    maxDistanceAccumulator.add((1, 0.0))

    var segmentPoints = mutable.ListBuffer[Point]()
    segmentPoints += points.head
    segmentPoints += points(1)

    var j = 2

    while (j < points.length) {
      val point = points(j)

      val projectedY = newYCoordinateOnSegment(point, (currentSegment._1, currentSegment._2))
      val projectedPoint = Point(point.x, projectedY)

      val distance = calculateDistance(point, projectedPoint)

      if (distance < threshold || j == points.length - 1) {
        currentSegment = (currentSegment._1, projectedPoint, currentSegment._3)

        segmentPoints += point

        val maxDistance = maxDistanceAccumulator.value.getOrElse(j, 0.0)
        if (distance > maxDistance) {
          maxDistanceAccumulator.add((j, distance))
        }

        j += 1  // Move to next point

      } else {

        val segmentSlope = calculateSlope((currentSegment._1, currentSegment._2))
        val isFluctuating = detectFluctuation(segmentPoints.toList, segmentSlope)

        segments :+= (currentSegment._1, currentSegment._2, segmentSlope, j, isFluctuating)

        if (j + 1 < points.length) {
          currentSegment = (points(j), points(j + 1), j)
          maxDistanceAccumulator.add((j, 0.0))
          maxDistanceAccumulator.add((j + 1, 0.0))

          // Reset segment points tracking
          segmentPoints.clear()
          segmentPoints += points(j)
          segmentPoints += points(j + 1)

          j += 2  // Skip the next point since it's used to define the new segment
        } else {
          // Last point: create single-point segment
          currentSegment = (points(j), points(j), j)
          maxDistanceAccumulator.add((j, 0.0))

          // Reset segment points tracking for single point
          segmentPoints.clear()
          segmentPoints += points(j)

          j += 1
        }
      }
    }

    val finalSegmentSlope = calculateSlope((currentSegment._1, currentSegment._2))
    val finalIsFluctuating = detectFluctuation(segmentPoints.toList, finalSegmentSlope)
    segments :+= (currentSegment._1, currentSegment._2, finalSegmentSlope, points.length - 1, finalIsFluctuating)
    segments
  }

  def detectFluctuation(segmentPoints: List[Point], expectedSlope: Double): Boolean = {
    if (segmentPoints.length < 3) return false  // Need at least 3 points to detect fluctuation

    val localSlopes = segmentPoints.sliding(2).map { case List(p1, p2) =>
      if (p2.x != p1.x) (p2.y - p1.y) / (p2.x - p1.x) else expectedSlope
    }.toList

    val fluctuationTolerance = if (math.abs(expectedSlope) > 0.01) {
      math.abs(expectedSlope) * 0.15  // 15% of slope magnitude
    } else {
      0.01  // Minimum absolute tolerance for near-zero slopes
    }

    val magnitudeFluctuation = localSlopes.exists(localSlope =>
      math.abs(localSlope - expectedSlope) > fluctuationTolerance)

    val directionChanges = if (expectedSlope > 0.01) {
      localSlopes.exists(_ < -0.01) // Decreasing steps in increasing segment
    } else if (expectedSlope < -0.01) {
      localSlopes.exists(_ > 0.01) // Increasing steps in decreasing segment
    } else false // For near-flat segments, only check magnitude

    magnitudeFluctuation || directionChanges
  }

  def processLines(lines: RDD[String]): RDD[(Int, List[Point])] = {
    lines.map { line =>
      val parts = line.split(";")
      val seriesId = parts(0).toInt
      val pointsString = parts(1).stripPrefix("MULTIPOINT(").stripSuffix(")")
      val points = pointsString.split(", ").map { pointString =>
        val pointParts = pointString.stripPrefix("(").stripSuffix(")").split(" ")
        Point(pointParts(0).toDouble, pointParts(1).toDouble)
      }.toList
      (seriesId, points)
    }
  }

  def generateGrid(w: Int, h: Double, yMin: Double, yMax: Double): Array[GridCell] = {
    val xMin = 0
    val xMax = 128  // Cover time indices 0 to 127

    val machines = Array("shark1", "shark2")
    val xSteps = (xMax - xMin) / w  // For w=4: 128/4 = 32 cells in x
    val ySteps = ((yMax - yMin) / h).toInt  // Number of cells in y dimension

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

  def isIntersecting(segment: (Double, Double, Double, Double), grid: GridCell): Boolean = {
    val (x1, y1, x2, y2) = segment
    def F(x: Double, y: Double): Double = {
      (y2 - y1) * x + (x1 - x2) * y + (x2 * y1 - x1 * y2)
    }
    val corners = List(
      (grid.xBL, grid.yBL),
      (grid.xTR, grid.yBL),
      (grid.xTR, grid.yTR),
      (grid.xBL, grid.yTR)
    )
    val cornerValues = corners.map { case (x, y) => F(x, y) }
    if (cornerValues.forall(_ > 0) || cornerValues.forall(_ < 0)) {
      return false
    }
    if (x1 > grid.xTR && x2 > grid.xTR) return false
    if (x1 < grid.xBL && x2 < grid.xBL) return false
    if (y1 > grid.yTR && y2 > grid.yTR) return false
    if (y1 < grid.yBL && y2 < grid.yBL) return false
    true
  }

  @deprecated("Use GridIndex.getIntersectingCells for better performance", "Aug 2025")
  def adjustedSegmentIntersectingGrids(segment: (Point, Point), grids: Array[GridCell], threshold: Double): List[GridCell] = {
    val minY = Math.min(segment._1.y, segment._2.y) - threshold
    val maxY = Math.max(segment._1.y, segment._2.y) + threshold
    val minX = Math.min(segment._1.x, segment._2.x) - threshold
    val maxX = Math.max(segment._1.x, segment._2.x) + threshold
    grids.filter { grid =>
      val intersectsY = (grid.yBL <= maxY) && (grid.yTR >= minY)
      val intersectsX = (grid.xBL <= maxX) && (grid.xTR >= minX)
      val containsEndPoint1 = grid.xBL <= segment._1.x && segment._1.x <= grid.xTR &&
        grid.yBL <= segment._1.y && segment._1.y <= grid.yTR
      val containsEndPoint2 = grid.xBL <= segment._2.x && segment._2.x <= grid.xTR &&
        grid.yBL <= segment._2.y && segment._2.y <= grid.yTR
      (intersectsY && intersectsX) || containsEndPoint1 || containsEndPoint2
    }.toList
  }

  class MachinePartitioner(machines: Set[String]) extends Partitioner {
    private val machineToIndex = machines.zipWithIndex.toMap
    val indexToMachine = machineToIndex.map(_.swap)
    logger.info(s"Machine to Index Mapping: ${machineToIndex.mkString(", ")}")
    logger.info(s"Index to Machine Mapping: ${indexToMachine.mkString(", ")}")
    override def numPartitions: Int = machines.size
    override def getPartition(key: Any): Int = {
      key match {
        case machine: String => machineToIndex.getOrElse(machine, 0)
        case _ => 0
      }
    }
    def getMachineName(index: Int): String = indexToMachine.getOrElse(index, "unknown")
  }

  class TablePartitioner(tables: Set[String]) extends Partitioner {
    private val tableToIndex = tables.zipWithIndex.toMap
    val indexToTable = tableToIndex.map(_.swap)
    logger.info(s"Table to Index Mapping: ${tableToIndex.mkString(", ")}")
    logger.info(s"Index to Table Mapping: ${indexToTable.mkString(", ")}")
    override def numPartitions: Int = tables.size
    override def getPartition(key: Any): Int = {
      key match {
        case tableIdentifier: String => tableToIndex.getOrElse(tableIdentifier, 0)
        case _ => 0
      }
    }
    def getTableName(index: Int): String = indexToTable.getOrElse(index, "unknown")
  }

  class MapAccumulator extends AccumulatorV2[(String, Int), mutable.Map[String, Int]] {
    private val _counts = mutable.Map[String, Int]().withDefaultValue(0)
    override def isZero: Boolean = _counts.isEmpty
    override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = {
      val newAcc = new MapAccumulator()
      _counts.synchronized {
        newAcc._counts ++= _counts
      }
      newAcc
    }
    override def reset(): Unit = _counts.clear()

    def addBatch(batch: Map[String, Int]): Unit = {
      _counts.synchronized {
        batch.foreach { case (k, v) =>
          _counts(k) += v
        }
      }
    }

    override def add(v: (String, Int)): Unit = {
      _counts.synchronized {
        _counts(v._1) += v._2
      }
    }
    override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {
      other match {
        case o: MapAccumulator =>
          o._counts.foreach { case (k, v) =>
            _counts.synchronized {
              _counts(k) += v
            }
          }
      }
    }
    override def value: mutable.Map[String, Int] = _counts
  }

  class MaxDistanceAccumulator extends AccumulatorV2[(Int, Double), mutable.Map[Int, Double]] {
    private val _maxDistances = mutable.Map[Int, Double]().withDefaultValue(0.0)
    override def isZero: Boolean = _maxDistances.isEmpty
    override def copy(): AccumulatorV2[(Int, Double), mutable.Map[Int, Double]] = {
      val newAcc = new MaxDistanceAccumulator()
      _maxDistances.synchronized {
        newAcc._maxDistances ++= _maxDistances
      }
      newAcc
    }
    override def reset(): Unit = _maxDistances.clear()

    def addBatch(batch: Map[Int, Double]): Unit = {
      _maxDistances.synchronized {
        batch.foreach { case (index, dist) =>
          _maxDistances(index) = math.max(_maxDistances(index), dist)
        }
      }
    }

    override def add(v: (Int, Double)): Unit = {
      _maxDistances.synchronized {
        _maxDistances(v._1) = math.max(_maxDistances(v._1), v._2)
      }
    }
    override def merge(other: AccumulatorV2[(Int, Double), mutable.Map[Int, Double]]): Unit = other match {
      case o: MaxDistanceAccumulator =>
        o._maxDistances.foreach { case (index, dist) =>
          _maxDistances(index) = math.max(_maxDistances.getOrElse(index, 0.0), dist)
        }
    }
    override def value: mutable.Map[Int, Double] = _maxDistances
  }

  def saveMapToHDFS[T, U](map: Map[T, U], filePath: String)(implicit spark: SparkSession): Unit = {
    val serializedMap = map.toList.map { case (key, value) => s"$key,$value" }.mkString("\n")
    val rdd = spark.sparkContext.parallelize(Seq(serializedMap))
    rdd.coalesce(1).saveAsTextFile(filePath)
  }

  def saveAccumulatorToHDFS(acc: AccumulatorV2[(String, Int), mutable.Map[String, Int]], filePath: String)(implicit spark: SparkSession): Unit = {
    saveMapToHDFS(acc.value.toMap, filePath)
  }

  def saveMaxDistanceAccumulatorToHDFS(acc: MaxDistanceAccumulator, filePath: String)(implicit spark: SparkSession): Unit = {
    if (!acc.isZero) {
      val serializedMap = acc.value.toList.map { case (index, maxDistance) => s"$index,$maxDistance" }.mkString("\n")
      val rdd = spark.sparkContext.parallelize(Seq(serializedMap))
      rdd.coalesce(1).saveAsTextFile(filePath)
      logger.info(s"Data saved to $filePath")
    } else {
      logger.warn("MaxDistanceAccumulator is empty, nothing to save.")
    }
  }

  def processTimeSeriesBatch(seriesData: Iterator[(String, Int, List[Point])],
                           threshold: Double,
                           gridIndex: GridIndex,
                           maxDistanceAccumulator: MaxDistanceAccumulator): (List[(String, Int, Point, Point, Double, Boolean)], Map[String, Int]) = {

    val gridCellCounts = mutable.Map[String, Int]().withDefaultValue(0)
    val segmentsList = mutable.ListBuffer[(String, Int, Point, Point, Double, Boolean)]()

    seriesData.foreach { case (table, seriesId, points) =>
      val segmentList = generateSegmentsList(points, threshold, maxDistanceAccumulator)

      segmentList.foreach { case (startPoint, endPoint, slope, index, isFluctuating) =>
        val intersectingGrids = gridIndex.getIntersectingCells((startPoint, endPoint), threshold)
        intersectingGrids.foreach(grid => gridCellCounts(grid.grid_id) += 1)

        segmentsList += ((table, seriesId, startPoint, endPoint, slope, isFluctuating))
      }
    }

    (segmentsList.toList, gridCellCounts.toMap)
  }

  def writeBufferToTableFile(buffer: mutable.ListBuffer[String], tableIdentifier: String): String = {
    logger.info(s"Starting optimized file writing for $tableIdentifier...")
    val fileWriteStartTime = System.nanoTime()
    val datasetDir = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_segments"
    val dirPath = s"/home/xli3/results/$datasetDir/"
    val dir = new File(dirPath)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val filePath = s"$dirPath${tableIdentifier}_time_series_segments.csv"
    val file = new File(filePath)

    val writer = new PrintWriter(new BufferedWriter(new FileWriter(file, false), 65536))
    try {
      buffer.foreach(writer.println)
    } finally {
      writer.close()
    }
    val fileWriteEndTime = System.nanoTime()
    logger.info(s"Optimized file writing for $tableIdentifier completed. Duration: ${(fileWriteEndTime - fileWriteStartTime) / 1e9} seconds")
    filePath
  }

  def ensureSegmentDirectoryExists(): Unit = {
    import scala.sys.process._

    val datasetDir = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_segments"
    val dirPath = s"/home/xli3/results/$datasetDir/"

    try {
      val dir = new File(dirPath)
      if (!dir.exists()) {
        logger.info(s"Creating segment directory: $dirPath")
        println(s"[DIRECTORY SETUP] Creating segment directory: $dirPath")

        val createCmd = s"mkdir -p $dirPath"
        val exitCode = createCmd.!

        if (exitCode == 0) {
          logger.info(s"Successfully created directory: $dirPath")
          println(s"[DIRECTORY SETUP] Successfully created directory: $dirPath")

          // Ensure ownership is correct (in case running as root)
          try {
            val chownCmd = s"chown xli3:xli3 $dirPath"
            chownCmd.!
            logger.info(s"Set ownership to xli3:xli3 for $dirPath")
            println(s"[DIRECTORY SETUP] Set ownership to xli3:xli3 for $dirPath")
          } catch {
            case e: Exception =>
              logger.warn(s"Could not change ownership (may not be needed): ${e.getMessage}")
              println(s"[DIRECTORY SETUP WARNING] Could not change ownership (may not be needed): ${e.getMessage}")
          }
        } else {
          logger.warn(s"Failed to create directory: $dirPath")
          println(s"[DIRECTORY SETUP WARNING] Failed to create directory: $dirPath")
        }
      } else {
        logger.info(s"Directory already exists: $dirPath")
        println(s"[DIRECTORY SETUP] Directory already exists: $dirPath")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error ensuring directory exists: ${e.getMessage}")
        println(s"[DIRECTORY SETUP ERROR] Failed to ensure directory exists: ${e.getMessage}")
    }
  }

  def cleanupSegmentCsvFiles(): Unit = {
    val datasetDir = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_segments"
    val segmentDirs = Seq(
      s"/home/xli3/results/$datasetDir/"
    )

    segmentDirs.foreach { dirPath =>
      try {
        val dir = new File(dirPath)
        if (dir.exists() && dir.isDirectory) {
          val csvFiles = dir.listFiles().filter(_.getName.endsWith("_time_series_segments.csv"))
          logger.info(s"Found ${csvFiles.length} segment CSV files in $dirPath")
          println(s"[CSV CLEANUP] Found ${csvFiles.length} segment CSV files in $dirPath")

          csvFiles.foreach { file =>
            if (file.delete()) {
              logger.info(s"Deleted: ${file.getName}")
              println(s"[CSV CLEANUP] Deleted: ${file.getName}")
            } else {
              logger.warn(s"Failed to delete: ${file.getName}")
              println(s"[CSV CLEANUP WARNING] Failed to delete: ${file.getName}")
            }
          }

          logger.info(s"Successfully cleaned up segment CSV files in $dirPath")
          println(s"[CSV CLEANUP] Successfully cleaned up segment CSV files in $dirPath")
        } else {
          logger.info(s"Directory does not exist or is not a directory: $dirPath")
          println(s"[CSV CLEANUP] Directory does not exist or is not a directory: $dirPath")
        }
      } catch {
        case e: Exception =>
          logger.error(s"Error cleaning up CSV files in $dirPath: ${e.getMessage}")
          println(s"[CSV CLEANUP ERROR] Failed to clean up CSV files in $dirPath: ${e.getMessage}")
      }
    }
  }

  def dropExistingPostgresTables(): Unit = {
    import java.sql.{Connection, DriverManager, Statement}

    val databases = Map(
      "shark1" -> "jdbc:postgresql://YOUR_SPARK_MASTER_HOST:5432/postgres",
      "shark2" -> "jdbc:postgresql://YOUR_DB_SERVER2_HOST:5432/postgres"
    )

    databases.foreach { case (host, url) =>
      var conn: Connection = null
      var stmt: Statement = null

      try {
        logger.info(s"Connecting to $host to drop existing ts_segments tables...")
        println(s"[POSTGRES CLEANUP] Connecting to $host to drop existing tables...")

        val props = new java.util.Properties()
        props.setProperty("user", "postgres")
        props.setProperty("password", "YOUR_PASSWORD_HERE")

        conn = DriverManager.getConnection(url, props)
        stmt = conn.createStatement()

        val tablePrefix = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_segments_%"
        val rs = stmt.executeQuery(
          s"SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '$tablePrefix'"
        )

        val tablesToDrop = scala.collection.mutable.ListBuffer[String]()
        while (rs.next()) {
          tablesToDrop += rs.getString("tablename")
        }
        rs.close()

        tablesToDrop.foreach { tableName =>
          val dropSQL = s"DROP TABLE IF EXISTS $tableName CASCADE"
          logger.info(s"Dropping table: $tableName on $host")
          println(s"[POSTGRES CLEANUP] Dropping table: $tableName on $host")
          stmt.execute(dropSQL)
        }

        logger.info(s"Successfully dropped ${tablesToDrop.size} tables on $host")
        println(s"[POSTGRES CLEANUP] Successfully dropped ${tablesToDrop.size} tables on $host")

      } catch {
        case e: Exception =>
          logger.error(s"Error dropping tables on $host: ${e.getMessage}")
          println(s"[POSTGRES CLEANUP ERROR] Failed to drop tables on $host: ${e.getMessage}")
      } finally {
        if (stmt != null) stmt.close()
        if (conn != null) conn.close()
      }
    }
  }

  def cleanupAndCreateMappingDirectory(spark: SparkSession): Unit = {
    val datasetDir = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_mapping"
    val mappingFilesPath = s"hdfs://shark1local:9000/user/xli3/output/$datasetDir"

    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      val path = new Path(mappingFilesPath)

      if (fs.exists(path)) {
        logger.info(s"Found existing mapping files directory: $mappingFilesPath")
        println(s"[HDFS CLEANUP] Found existing mapping files directory: $mappingFilesPath")
        logger.info("Cleaning up existing mapping files...")
        println("[HDFS CLEANUP] Cleaning up existing mapping files...")

        val deleteSuccess = fs.delete(path, true) // true = recursive delete

        if (deleteSuccess) {
          logger.info(s"Successfully deleted existing mapping files directory: $mappingFilesPath")
          println(s"[HDFS CLEANUP] Successfully deleted existing mapping files directory: $mappingFilesPath")
        } else {
          logger.warn(s"Failed to delete existing mapping files directory: $mappingFilesPath")
          println(s"[HDFS CLEANUP WARNING] Failed to delete existing mapping files directory: $mappingFilesPath")
        }
      } else {
        logger.info(s"Mapping files directory does not exist: $mappingFilesPath")
        println(s"[HDFS CLEANUP] Mapping files directory does not exist: $mappingFilesPath")
      }

      val createSuccess = fs.mkdirs(path)

      if (createSuccess) {
        logger.info(s"Successfully created mapping files directory: $mappingFilesPath")
        println(s"[HDFS CLEANUP] Successfully created mapping files directory: $mappingFilesPath")
      } else {
        logger.warn(s"Failed to create mapping files directory: $mappingFilesPath")
        println(s"[HDFS CLEANUP WARNING] Failed to create mapping files directory: $mappingFilesPath")
      }

      fs.close()

    } catch {
      case e: Exception =>
        logger.error(s"Error during HDFS directory cleanup/creation: ${e.getMessage}", e)
        throw new RuntimeException(s"Failed to setup mapping files directory: ${e.getMessage}", e)
    }
  }

  def main(args: Array[String]): Unit = {
    val DATASET_EARLY = Config.DATASET
    val DATASET_SIZE_EARLY = Config.DATASET_SIZE
    val ALPHA_EARLY = Config.ALPHA
    val params_EARLY = Config.getParams()

    val numTablesEarly = DATASET_SIZE_EARLY match {
      case "25m" => 40
      case "50m" => 80
      case "100m" => 160
      case "200m" => 320
      case _ => 40
    }

    val appName = f"Preprocessing_${DATASET_SIZE_EARLY}_128_${numTablesEarly}T_${DATASET_EARLY}_th${params_EARLY.threshold}%.2f_alpha${ALPHA_EARLY}%.2f_grid${params_EARLY.gridW}x${params_EARLY.gridH}"

    val conf = new SparkConf().setAppName(appName).setMaster("spark://YOUR_SPARK_MASTER_HOST:7077")
    conf.set("spark.executor.memory", "15g")
    conf.set("spark.driver.memory", "16g")   
    conf.set("spark.executor.memoryOverhead", "3g")
    conf.set("spark.executor.cores", "4")      
    conf.set("spark.executor.instances", "26")
    conf.set("spark.default.parallelism", "208")
    conf.set("spark.sql.shuffle.partitions", "208")
    conf.set("dfs.client.use.datanode.hostname", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.local.dir", "/home/xli3/spark-temp")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "/home/xli3/spark-eventlogs")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    implicit val implicitSpark: SparkSession = spark

    logger.info("=== ENSURING SEGMENT DIRECTORY EXISTS ===")
    ensureSegmentDirectoryExists()
    logger.info("=== SEGMENT DIRECTORY SETUP COMPLETED ===")

    logger.info("=== STARTING AUTOMATIC CSV SEGMENT FILES CLEANUP ===")
    cleanupSegmentCsvFiles()
    logger.info("=== CSV SEGMENT FILES CLEANUP COMPLETED ===")

    logger.info("=== STARTING AUTOMATIC POSTGRES CLEANUP ===")
    dropExistingPostgresTables()
    logger.info("=== POSTGRES CLEANUP COMPLETED ===")

    logger.info("=== STARTING AUTOMATIC HDFS CLEANUP ===")
    cleanupAndCreateMappingDirectory(spark)
    logger.info("=== HDFS CLEANUP COMPLETED ===")

    Class.forName("org.postgresql.Driver")

    val DATASET = Config.DATASET
    val DATASET_SIZE = Config.DATASET_SIZE
    val ALPHA = Config.ALPHA
    val params = Config.getParams()

    val threshold = params.threshold
    val gridW = params.gridW
    val gridH = params.gridH
    val yMin = params.yMin
    val yMax = params.yMax

    logger.info(s"=== DATASET: $DATASET $DATASET_SIZE ===")
    logger.info(s"Threshold: $threshold")
    logger.info(s"Grid parameters: w=$gridW, h=$gridH, y-range=[$yMin, $yMax]")

    val gridCells = generateGrid(gridW, gridH, yMin, yMax)
    val gridIndex = GridIndex(gridCells)
    logger.info(s"Created GridIndex with ${gridCells.length} cells for optimized intersection testing")

    val gridCellAccumulator = new MapAccumulator()
    sc.register(gridCellAccumulator, "Grid Cell Intersections")

    val maxDistanceAccumulator = new MaxDistanceAccumulator()
    sc.register(maxDistanceAccumulator, "Max Distance Per Timestamp")

    val numFiles = DATASET_SIZE match {
      case "25m" => 40
      case "50m" => 80
      case "100m" => 160
      case "200m" => 320
      case _ => throw new IllegalArgumentException(s"Unknown dataset size: $DATASET_SIZE")
    }

    val formattedSize = DATASET_SIZE match {
      case "25m" => "025M"
      case "50m" => "050M"
      case "100m" => "100M"
      case "200m" => "200M"
      case _ => DATASET_SIZE.toUpperCase
    }

    val csvFileToTableMap = DATASET match {
      case "TSBS" => (1 to numFiles).map(i => s"time_series_${DATASET_SIZE.toUpperCase}_$i.csv" -> s"table$i").toMap
      case "ECG"  => (1 to numFiles).map(i => s"time_series_${formattedSize}_$i.csv" -> s"table$i").toMap
      case "RANDOMWALK" => (0 until numFiles).map(i => s"time_series_${formattedSize}_$i.csv" -> s"table${i+1}").toMap
      case _ => throw new IllegalArgumentException(s"Unknown dataset: $DATASET")
    }

    val seriesRDDs = csvFileToTableMap.map { case (fileName, table) =>
      val filePath = DATASET match {
        case "TSBS" => s"hdfs://shark1local:9000/user/xli3/input/tsbs/tsbs_fuel_${DATASET_SIZE}_${numFiles}_tables/$fileName"
        case "ECG"  => s"hdfs://shark1local:9000/user/xli3/input/ecg/ecg_0${DATASET_SIZE}_${numFiles}_files/$fileName"
        case "RANDOMWALK" => s"hdfs://shark1local:9000/user/xli3/input/random_walk/randwalk_${DATASET_SIZE}_${numFiles}_tables/$fileName"
        case _ => throw new IllegalArgumentException(s"Unknown dataset: $DATASET")
      }
      processLines(sc.textFile(filePath)).map { case (seriesId, points) =>
        (table, seriesId, points)
      }
    }
    val unionSeriesRDD = sc.union(seriesRDDs.toSeq)

    val segmentationStartTime = System.currentTimeMillis()

    val performIntersectionChecks = true

    val segmentsRDD = unionSeriesRDD.mapPartitions { partition =>
      logger.info("Starting batch processing for partition")
      val (segmentsList, gridCounts) = processTimeSeriesBatch(partition, threshold, gridIndex, maxDistanceAccumulator)

      if (gridCounts.nonEmpty) {
        gridCellAccumulator.addBatch(gridCounts)
        logger.info(s"Added ${gridCounts.size} grid cell counts to accumulator in batch")
      }

      segmentsList.iterator.map { case (table, seriesId, startPoint, endPoint, slope, isFluctuating) =>
        (table, (seriesId, (startPoint, endPoint), slope, isFluctuating))
      }
    }

    val segmentationEndTime = System.currentTimeMillis()
    logger.info(s"Optimized Hash-based Segmentation with Grid Intersections process took ${(segmentationEndTime - segmentationStartTime) / 1000.0} seconds")

    logger.info("Starting the partition output stage.")
    val partitionOutputStartTime = System.currentTimeMillis()

    val allTableIdentifiers = (1 to numFiles).map(i => s"table$i").toSet
    val tablePartitioner = new TablePartitioner(allTableIdentifiers)
    logger.info(s"Partition Index to Table Mapping: ${tablePartitioner.indexToTable.mkString(", ")}")
    logger.info("Starting repartitioning...")
    val repartitionStartTime = System.currentTimeMillis()

    val partitionedByTable = segmentsRDD.partitionBy(tablePartitioner)
    val repartitionEndTime = System.currentTimeMillis()
    logger.info(s"Repartitioning completed. Duration: ${(repartitionEndTime - repartitionStartTime) / 1000.0} seconds")

    logger.info("Starting collect operation to gather file paths and host names...")
    val collectStartTime = System.currentTimeMillis()

    val csvFileToHostMap = partitionedByTable.mapPartitionsWithIndex { (index, partitionIterator) =>
      val segmentsBuffer = new mutable.ListBuffer[String]()
      val tableIdentifier = tablePartitioner.getTableName(index)
      partitionIterator.foreach { case (_, (seriesId, (startPoint, endPoint), slope, isFluctuating)) =>
        val segmentString = f"$seriesId;LINESTRING(${startPoint.x}%.2f ${startPoint.y}%.2f, " +
          f"${endPoint.x}%.2f ${endPoint.y}%.2f);$slope%.2f;$isFluctuating"
        segmentsBuffer += segmentString
      }
      if (segmentsBuffer.nonEmpty) {
        val filePath = writeBufferToTableFile(segmentsBuffer, tableIdentifier)
        val hostAddress = org.apache.spark.SparkEnv.get.blockManager.blockManagerId.host
        val hostName = try {
          val inetAddress = InetAddress.getByName(hostAddress)
          val hostNameFull = inetAddress.getCanonicalHostName
          if (hostNameFull == hostAddress || hostNameFull.matches("\\d+\\.\\d+\\.\\d+\\.\\d+"))
            hostAddress
          else
            hostNameFull.split("\\.")(0)
        } catch {
          case e: Exception =>
            logger.error(s"Failed to resolve hostname for IP address $hostAddress. Using IP address as fallback.", e)
            hostAddress
        }
        logger.info(s"Partition processed on host: $hostName (IP: $hostAddress)")
        Iterator((filePath, hostName))
      } else {
        Iterator.empty
      }
    }.collectAsMap()

    val collectEndTime = System.currentTimeMillis()
    logger.info(s"Collect operation completed. Duration: ${(collectEndTime - collectStartTime) / 1000.0} seconds")

    logger.info("Output CSV files and their host machines:")
    csvFileToHostMap.foreach { case (filePath, hostName) =>
      logger.info(s"File: $filePath, Host: $hostName")
    }

    val partitionOutputEndTime = System.currentTimeMillis()
    logger.info(s"Partitioning & TS segments output processing took ${(partitionOutputEndTime - partitionOutputStartTime) / 1000.0} seconds")

    logger.info("=== SAVING RESULTS TO CLEANED HDFS DIRECTORY ===")

    val datasetMappingDir = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_mapping"
    val mappingBasePath = s"hdfs://shark1local:9000/user/xli3/output/$datasetMappingDir"

    saveAccumulatorToHDFS(gridCellAccumulator, s"$mappingBasePath/gridCellAccumulator")
    logger.info(s"Max Distance Accumulator content before saving: ${maxDistanceAccumulator.value.size} entries")
    logger.info("Saving Max Distance Accumulator to HDFS...")
    saveMaxDistanceAccumulatorToHDFS(maxDistanceAccumulator, s"$mappingBasePath/max_distance")
    logger.info("Max Distance Accumulator saved.")

    val tableCreationStartTime = System.currentTimeMillis()

    val databases: Map[String, String] = Map(
      "shark1"          -> "jdbc:postgresql://YOUR_SPARK_MASTER_HOST:5432/postgres",
      "shark2"          -> "jdbc:postgresql://YOUR_DB_SERVER2_HOST:5432/postgres",
      "YOUR_DB_SERVER1_IP"  -> "jdbc:postgresql://YOUR_SPARK_MASTER_HOST:5432/postgres",
      "YOUR_DB_SERVER2_IP"   -> "jdbc:postgresql://YOUR_DB_SERVER2_HOST:5432/postgres"
    )

    val tableCreationLogger = org.apache.log4j.Logger.getLogger(this.getClass)
    logger.info(s"Starting table creation. Total CSV files: ${csvFileToHostMap.size}")
    csvFileToHostMap.foreach { case (filePath, hostMachine) =>
      logger.info(s"CSV File: $filePath, Host Machine: $hostMachine")
    }

    csvFileToHostMap.foreach { case (filePath, hostMachine) =>
      val url = databases.getOrElse(hostMachine, throw new IllegalArgumentException(s"No database URL found for host machine: $hostMachine"))
      val tableIdentifier = filePath.split("/").last.replace("_time_series_segments.csv", "")
      val tableName = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_segments_$tableIdentifier"

      try {
        tableCreationLogger.info(s"Creating table: $tableName at $url on host: $hostMachine")

        val properties = new java.util.Properties()
        properties.setProperty("user", "postgres")
        properties.setProperty("password", "YOUR_PASSWORD_HERE")
        val connection = DriverManager.getConnection(url, properties)
        val statement = connection.createStatement()

        statement.executeUpdate(s"DROP TABLE IF EXISTS $tableName")

        statement.executeUpdate(
          s"CREATE TABLE IF NOT EXISTS $tableName (" +
            "time_series_id INTEGER," +
            "time_series_segments GEOMETRY(LineString, 4326)," +
            "segment_slope DOUBLE PRECISION," +
            "is_fluctuating BOOLEAN" +
            ")"
        )

        statement.executeUpdate(s"CREATE INDEX IF NOT EXISTS index_${tableName}_ts_seg ON $tableName USING GIST (time_series_segments);")

        statement.executeUpdate(s"CREATE INDEX IF NOT EXISTS index_${tableName}_slope ON $tableName (segment_slope);")

        statement.executeUpdate(s"CREATE INDEX IF NOT EXISTS index_${tableName}_fluctuation ON $tableName (is_fluctuating);")

        connection.close()
        tableCreationLogger.info(s"Table $tableName created successfully on host: $hostMachine.")
      } catch {
        case e: Exception =>
          tableCreationLogger.error("Error during table creation: ", e)
      }
    }
    tableCreationLogger.info("Completed table creation process")

    val datasetPrefix = s"${Config.DATASET.toLowerCase}_${Config.DATASET_SIZE}_segments"
    val csvFileToHostMappingString = csvFileToHostMap
      .map { case (csvPath, host) =>
        val tableIdentifier = csvPath.split("/").last.replace("_time_series_segments.csv", "")
        val fullTableName = s"${datasetPrefix}_$tableIdentifier"
        s"$fullTableName,$host"
      }
      .mkString("\n")
    val csvFileToHostMappingRDD = spark.sparkContext.parallelize(Seq(csvFileToHostMappingString))
    val repartitionedRDD = csvFileToHostMappingRDD.repartition(1)

    repartitionedRDD.saveAsTextFile(s"hdfs://shark1local:9000/user/xli3/output/$datasetMappingDir/tableToHostMapping")

    val tableCreationEndTime = System.currentTimeMillis()
    logger.info(s"Table and index creation process took ${(tableCreationEndTime - tableCreationStartTime) / 1000.0} seconds")

    val totalEndTime = System.currentTimeMillis()
    val totalDuration = (totalEndTime - segmentationStartTime) / 1000.0
    logger.info(s"Preprocessing completed in ${totalDuration} seconds")

    spark.stop()
  } // End of main method

} // End of the program