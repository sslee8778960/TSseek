package org.example.bigdata

/**
 * Configuration File
 *
 * Users only need to modify the three parameters in the USER CONFIGURATION section.
 * All other parameters are automatically derived based on the dataset and alpha value.
 */
object Config {


  // USER CONFIGURATION - Please modify these 3 parameters only


  val DATASET = "TSBS"        // Options: "TSBS", "ECG", "RANDOMWALK"
  val DATASET_SIZE = "25m"    // Options: "25m", "50m", "100m", "200m"
  val ALPHA = 0.6             // Segmentation threshold multiplier
  

  // QUERY PATTERNS - Please modify it with your own query


  val TSBS_PATTERN = "| [12, 42], <+>, *0.95*, {32} |, | [42, 12], <->, *1.00*, {32} |, | [10, 38], <+>, *0.25*, {24, 25} |, | [38, 12], <->, *0.30*, {39, 40} |"

  val ECG_PATTERN = "| [-0.45, 0.85], <+>, *0.03*, {32} |, | [0.85, -0.45], <->, *0.04*, {32} |, | [-0.20, 1.10], <+>, *0.02*, {24, 25} |, | [1.10, -0.60], <->, *0.03*, {39, 40} |"

  val RANDOMWALK_PATTERN = "| [-4.5, -2.5], <+>, *0.01*, {32} |, | [-0.6, 1.8], <->, *0.02*, {32} |, | [-2.0, 2.0], <+>, *0.01*, {24, 25} |, | [2.0, 4.0], <->, *0.02*, {39, 40} |"


  // DATASET PARAMETERS

  case class DatasetParams(
    threshold: Double,
    span90: Double,
    gridW: Int,
    gridH: Double,
    yMin: Double,
    yMax: Double,
    queryPattern: String,
    epsilon: Double
  )

  // DATASET STATISTICS - Please update it with your own dataset statistics

  def getParams(dataset: String = DATASET, alpha: Double = ALPHA): DatasetParams = {
    dataset match {
      case "TSBS" =>
        val span90 = 45.3
        val threshold = alpha * span90
        DatasetParams(
          threshold = threshold,
          span90 = span90,
          gridW = 8,
          gridH = 15.0,
          yMin = 0.0,
          yMax = 50.0,
          queryPattern = TSBS_PATTERN,
          epsilon = threshold
        )

      case "ECG" =>
        val span90 = 0.30
        val threshold = alpha * span90
        DatasetParams(
          threshold = threshold,
          span90 = span90,
          gridW = 8,
          gridH = 0.25,
          yMin = -2.0,
          yMax = 3.5,
          queryPattern = ECG_PATTERN,
          epsilon = threshold
        )

      case "RANDOMWALK" =>
        val span90 = 3.18
        val threshold = alpha * span90
        DatasetParams(
          threshold = threshold,
          span90 = span90,
          gridW = 8,
          gridH = 0.25,
          yMin = -10.0,
          yMax = 10.0,
          queryPattern = RANDOMWALK_PATTERN,
          epsilon = threshold
        )

      case _ => throw new IllegalArgumentException(s"Unknown dataset: $dataset")
    }
  }


}
