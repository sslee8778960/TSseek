package org.example.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager, Statement}
import scala.sys.process._
import java.io.File

object PostgresDataLoader {

  private val logger = LoggerFactory.getLogger(PostgresDataLoader.getClass)

  val DATASET = Config.DATASET
  val DATASET_SIZE = Config.DATASET_SIZE

  val TABLE_PREFIX = s"${DATASET.toLowerCase}_${DATASET_SIZE}_og_ts"

  val NUM_TABLES = DATASET_SIZE match {
    case "25m" => 40
    case "50m" => 80
    case "100m" => 160
    case "200m" => 320
    case _ => throw new IllegalArgumentException(s"Unknown dataset size: $DATASET_SIZE")
  }

  val DB_NAME = "postgres"
  val DB_USER = "postgres"
  val DB_PASSWORD = "YOUR_PASSWORD_HERE"

  val SHARK1 = "shark1"
  val SHARK2 = "shark2"

  val tableToMachine: Map[Int, String] =
    (1 to NUM_TABLES/2).map(_ -> SHARK1).toMap ++
    ((NUM_TABLES/2 + 1) to NUM_TABLES).map(_ -> SHARK2).toMap

  def main(args: Array[String]): Unit = {
    println("=" * 80)
    println("=== ORIGINAL TIME SERIES DATA DISTRIBUTOR ===")
    println(s"=== Dataset: $DATASET $DATASET_SIZE ===")
    println(s"=== Distributing $NUM_TABLES tables across shark1 (1-${NUM_TABLES/2}) and shark2 (${NUM_TABLES/2+1}-$NUM_TABLES) ===")
    println("=" * 80)

    val overallStartTime = System.nanoTime()

    val conf = new SparkConf()
      .setAppName(s"Original TS Data Distributor - $DATASET $DATASET_SIZE")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "2g")
      .set("spark.executor.cores", "4")
      .set("spark.default.parallelism", NUM_TABLES.toString)
      .set("spark.sql.shuffle.partitions", NUM_TABLES.toString)
      .set("spark.network.timeout", "800s")
      .set("spark.executor.heartbeatInterval", "60s")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    try {
      val formattedSize = DATASET_SIZE match {
        case "25m" => "025M"
        case "50m" => "050M"
        case "100m" => "100M"
        case "200m" => "200M"
        case _ => DATASET_SIZE.toUpperCase
      }

      val (hdfsBasePath, fileNamePattern) = DATASET match {
        case "TSBS" =>
          (s"hdfs://shark1local:9000/user/xli3/input/tsbs/tsbs_fuel_${DATASET_SIZE}_${NUM_TABLES}_tables",
           (1 to NUM_TABLES).map(i => (s"time_series_${DATASET_SIZE.toUpperCase}_$i.csv", s"table$i")))
        case "ECG" =>
          (s"hdfs://shark1local:9000/user/xli3/input/ecg/ecg_0${DATASET_SIZE}_${NUM_TABLES}_files",
           (1 to NUM_TABLES).map(i => (s"time_series_${formattedSize}_$i.csv", s"table$i")))
        case "RANDOMWALK" =>
          (s"hdfs://shark1local:9000/user/xli3/input/random_walk/randwalk_${DATASET_SIZE}_${NUM_TABLES}_tables",
           (0 until NUM_TABLES).map(i => (s"time_series_${formattedSize}_$i.csv", s"table${i+1}")))
        case _ => throw new IllegalArgumentException(s"Unknown dataset: $DATASET")
      }

      println("\n" + "=" * 80)
      println("STEP 1: Cleaning up existing tables (if any)...")
      println("=" * 80)

      for (tableNum <- 1 to NUM_TABLES) {
        val tableName = s"${TABLE_PREFIX}_table$tableNum"
        val machine = tableToMachine(tableNum)
        println(s"  → Dropping $tableName on $machine (if exists)...")
        dropTableIfExists(machine, tableName)
      }

      println("\n✓ Cleanup complete")

      println("\n" + "=" * 80)
      println("STEP 2: Creating and populating tables...")
      println("=" * 80)

      val fileTablePairs = fileNamePattern.zipWithIndex.map { case ((fileName, tableName), idx) =>
        val tableNum = idx + 1
        val filePath = s"$hdfsBasePath/$fileName"
        val machine = tableToMachine(tableNum)
        (filePath, tableName, tableNum, machine)
      }

      fileTablePairs.foreach { case (filePath, tableName, tableNum, machine) =>
        processFile(spark, filePath, tableName, tableNum, machine)
      }

      println("\n" + "=" * 80)
      println("STEP 3: Verifying table counts...")
      println("=" * 80)

      for (tableNum <- 1 to NUM_TABLES) {
        val tableName = s"${TABLE_PREFIX}_table$tableNum"
        val machine = tableToMachine(tableNum)
        val count = getTableCount(machine, tableName)
        println(f"  ✓ $tableName on $machine: $count%,d rows")
      }

      val overallEndTime = System.nanoTime()
      val totalDuration = (overallEndTime - overallStartTime) / 1e9d / 60.0 // minutes

      println("\n" + "=" * 80)
      println(s"✓ DISTRIBUTION COMPLETE!")
      println(f"Total time: $totalDuration%.2f minutes")
      println("=" * 80)

    } finally {
      spark.stop()
    }
  }

  def processFile(spark: SparkSession, hdfsPath: String, tableName: String, tableNum: Int, machine: String): Unit = {
    val startTime = System.nanoTime()
    val pgTableName = s"${TABLE_PREFIX}_table$tableNum"

    println(s"\n[$machine] Processing $tableName → $pgTableName")

    try {
      println(s"  → Creating table $pgTableName on $machine")
      createTable(machine, pgTableName)

      println(s"  → Reading from HDFS: $hdfsPath")
      import spark.implicits._

      val df = spark.sparkContext.textFile(hdfsPath)
        .map { line =>
          val parts = line.split(";", 2)
          if (parts.length == 2) {
            (parts(0).toLong, parts(1))
          } else {
            throw new IllegalArgumentException(s"Invalid CSV format: $line")
          }
        }
        .toDF("ts_id", "ts_data")

      val rowCount = df.count()
      println(s"  → Read $rowCount rows from HDFS")

      println(s"  → Writing to PostgreSQL using Spark JDBC...")
      val jdbcUrl = s"jdbc:postgresql://$machine:5432/$DB_NAME"
      val connectionProperties = new java.util.Properties()
      connectionProperties.put("user", DB_USER)
      connectionProperties.put("password", DB_PASSWORD)
      connectionProperties.put("driver", "org.postgresql.Driver")

      df.write
        .mode("append")
        .jdbc(jdbcUrl, pgTableName, connectionProperties)

      println(s"  → Write complete")

      println(s"  → Creating B-tree index on ts_id...")
      createIndex(machine, pgTableName)

      val endTime = System.nanoTime()
      val duration = (endTime - startTime) / 1e9d / 60.0 // minutes

      println(f"  ✓ Completed $pgTableName in $duration%.2f minutes")

    } catch {
      case e: Exception =>
        println(s"  ✗ ERROR processing $pgTableName: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def dropTableIfExists(machine: String, tableName: String): Unit = {
    val sql = s"DROP TABLE IF EXISTS $tableName CASCADE"
    executeSQL(machine, sql)
  }

  def createTable(machine: String, tableName: String): Unit = {
    val sql = s"""
      CREATE TABLE $tableName (
        ts_id BIGINT PRIMARY KEY,
        ts_data TEXT
      )
    """
    executeSQL(machine, sql)
  }

  def createIndex(machine: String, tableName: String): Unit = {
    val indexName = s"idx_${tableName}_tsid"
    val sql = s"CREATE INDEX $indexName ON $tableName(ts_id)"
    executeSQL(machine, sql)
  }

  def getTableCount(machine: String, tableName: String): Long = {
    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    var conn: Connection = null

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(s"SELECT COUNT(*) FROM $tableName")
      rs.next()
      val count = rs.getLong(1)
      rs.close()
      stmt.close()
      count
    } finally {
      if (conn != null) conn.close()
    }
  }

  def executeSQL(machine: String, sql: String): Unit = {
    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    var conn: Connection = null

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      val stmt = conn.createStatement()
      stmt.execute(sql)
      stmt.close()
    } finally {
      if (conn != null) conn.close()
    }
  }
}
