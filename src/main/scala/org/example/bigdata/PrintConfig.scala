package org.example.bigdata

object PrintConfig {
  def main(args: Array[String]): Unit = {
    println(s"DATASET=${Config.DATASET}")
    println(s"DATASET_SIZE=${Config.DATASET_SIZE}")
    println(s"ALPHA=${Config.ALPHA}")
    println(s"MATCHING_TYPE=${Config.MATCHING_TYPE}")
    println(s"SUBSEQUENCE_CASE=${Config.SUBSEQUENCE_CASE}")
    println(s"MULTI_PATTERN_STRATEGY=${Config.MULTI_PATTERN_STRATEGY}")
    println(s"USE_FLUCTUATION_FILTER=${Config.USE_FLUCTUATION_FILTER}")
  }
}
