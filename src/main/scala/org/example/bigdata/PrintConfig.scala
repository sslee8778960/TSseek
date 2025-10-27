package org.example.bigdata

object PrintConfig {
  def main(args: Array[String]): Unit = {
    println(s"DATASET=${Config.DATASET}")
    println(s"DATASET_SIZE=${Config.DATASET_SIZE}")
    println(s"ALPHA=${Config.ALPHA}")
  }
}
