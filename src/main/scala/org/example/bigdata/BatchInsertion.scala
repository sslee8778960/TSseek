package org.example.bigdata

import java.io.File
import java.net.InetAddress
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.sys.process._
import scala.util.Try

object BatchInsertion {
  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    val dataset = Config.DATASET
    val datasetSize = Config.DATASET_SIZE

    println(s"=== Batch Insertion Configuration ===")
    println(s"Dataset: $dataset")
    println(s"Dataset Size: $datasetSize")

    val datasetDir = s"${dataset.toLowerCase}_${datasetSize}_segments"
    val csvDirectoryPath = s"/home/xli3/results/$datasetDir"
    val databaseName = "postgres"
    val dbUser = "postgres"
    val dbPassword = "YOUR_PASSWORD_HERE"

    val localHostName = InetAddress.getLocalHost.getHostName.split("\\.")(0)
    println(s"Running batch insertion on $localHostName")
    println(s"Reading CSV files from: $csvDirectoryPath")

    val csvDirectory = new File(csvDirectoryPath)
    if (!csvDirectory.exists() || !csvDirectory.isDirectory) {
      System.err.println(s"ERROR: Directory does not exist: $csvDirectoryPath")
      System.err.println("Please run preprocessing first to generate segment CSV files.")
      System.exit(1)
    }

    val csvFiles = csvDirectory
      .listFiles
      .filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .filter(_.getName.contains("time_series_segments"))

    if (csvFiles.isEmpty) {
      System.err.println(s"ERROR: No CSV segment files found in $csvDirectoryPath")
      System.err.println("Please run preprocessing first to generate segment CSV files.")
      System.exit(1)
    }

    println(s"Found ${csvFiles.length} CSV files to process")

    val numThreads = 6
    val executor: ExecutorService = Executors.newFixedThreadPool(numThreads)

    def processFile(file: File): Unit = {
      val fileName = file.getName
      val tableIdentifier = fileName.split("_").dropRight(3).mkString("_")

      val tableName = s"${dataset.toLowerCase}_${datasetSize}_segments_$tableIdentifier"

      val commandString =
        s"""SET synchronous_commit = off; \\copy $tableName(time_series_id, time_series_segments, segment_slope, is_fluctuating) FROM '${file.getAbsolutePath}' WITH (FORMAT csv, HEADER false, DELIMITER ';')"""
      val psqlCommand = s"""echo "$commandString" | psql -U $dbUser -d $databaseName"""

      println(s"Executing: $psqlCommand")

      val command = Seq("bash", "-c", psqlCommand)
      val envVars = Map("PGPASSWORD" -> dbPassword)

      val exitCode = Process(command, None, envVars.toSeq: _*).!

      if (exitCode == 0) {
        println(s"✓ Successfully inserted data from ${file.getAbsolutePath} into $tableName.")
      } else {
        System.err.println(s"✗ Failed to insert data from ${file.getAbsolutePath} into $tableName.")
      }
    }

    csvFiles.foreach { file =>
      executor.submit(new Runnable {
        def run(): Unit = processFile(file)
      })
    }

    executor.shutdown()
    try {
      if (!executor.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)) {
        executor.shutdownNow()
      }
    } catch {
      case e: InterruptedException =>
        executor.shutdownNow()
        Thread.currentThread().interrupt()
    }

    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d // seconds

    println(s"=== Batch insertion completed in $duration seconds ===")
  }
}
