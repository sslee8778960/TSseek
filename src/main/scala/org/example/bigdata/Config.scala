package org.example.bigdata

/**
 * Configuration File
 *
 * Users only need to modify the parameters in the USER CONFIGURATION section.
 * All other parameters are automatically derived based on the dataset and alpha value.
 */
object Config {


  // USER CONFIGURATION - Please modify these parameters only


  val DATASET = "TSBS"        // Options: "TSBS", "ECG", "RANDOMWALK"
  val DATASET_SIZE = "25m"    // Options: "25m", "50m", "100m", "200m"
  val ALPHA = 0.6             // Segmentation threshold multiplier

  val MATCHING_TYPE = "WHOLE_SEQUENCE"   // Options: "WHOLE_SEQUENCE", "SUBSEQUENCE"

  // Subsequence query case, used when MATCHING_TYPE = "SUBSEQUENCE".
  // Single-pattern cases:
  //   "FREE_FIXED_LEN", "FREE_DYN_LEN", "BOUNDED_FIXED_LEN", "BOUNDED_DYN_LEN",
  //   "FIXED_START_FIXED_LEN", "FIXED_START_DYN_LEN"
  // Multi-pattern cases:
  //   "MULTI_ADJACENT", "MULTI_FIXED_GAP", "MULTI_RANGE_GAP",
  //   "MULTI_ORDERED_FLEX_GAP", "MULTI_UNORDERED_FLEX_GAP"
  val SUBSEQUENCE_CASE = "FREE_FIXED_LEN"

  val MULTI_PATTERN_STRATEGY = "CASCADE"  // Options: "INTERSECT_ALL", "BEST_ONLY", "CASCADE"

  val USE_FLUCTUATION_FILTER = true       // Include the is_fluctuating filter in Stage-1 SQL


  // WHOLE-MATCHING QUERY PATTERNS - Please modify it with your own query


  val TSBS_PATTERN = "| [12, 42], <+>, *0.95*, {32} |, | [42, 12], <->, *1.00*, {32} |, | [10, 38], <+>, *0.25*, {24, 25} |, | [38, 12], <->, *0.30*, {39, 40} |"

  val ECG_PATTERN = "| [-0.45, 0.85], <+>, *0.03*, {32} |, | [0.85, -0.45], <->, *0.04*, {32} |, | [-0.20, 1.10], <+>, *0.02*, {24, 25} |, | [1.10, -0.60], <->, *0.03*, {39, 40} |"

  val RANDOMWALK_PATTERN = "| [-4.5, -2.5], <+>, *0.01*, {32} |, | [-0.6, 1.8], <->, *0.02*, {32} |, | [-2.0, 2.0], <+>, *0.01*, {24, 25} |, | [2.0, 4.0], <->, *0.02*, {39, 40} |"


  // SUBSEQUENCE QUERY PATTERNS - Please modify it with your own query
  // Format: | [range], <direction>, step, {length} | [@position] Threshold: N
  //   @N for a fixed start position, @[a, b] for a bounded start range
  //   #N# fixed gap, #N-M# gap range, .... any gap, "UNORDERED:" prefix for unordered patterns


  // ECG subsequence query cases
  val ECG_FREE_FIXED_LEN         = "| [-0.30, -0.18], <+>, **, {5} | Threshold: 0"
  val ECG_FREE_DYN_LEN           = "PUT YOUR QUERY HERE"
  val ECG_FREE_DYN_LEN_WIDE      = "PUT YOUR QUERY HERE"
  val ECG_BOUNDED_FIXED_LEN      = "PUT YOUR QUERY HERE"
  val ECG_BOUNDED_DYN_LEN        = "PUT YOUR QUERY HERE"
  val ECG_FIXED_START_FIXED_LEN  = "PUT YOUR QUERY HERE"
  val ECG_FIXED_START_DYN_LEN    = "PUT YOUR QUERY HERE"
  val ECG_MULTI_ADJACENT         = "| [-0.35, -0.18], <+>, **, {5} |, #0#, | [-0.35, -0.18], <->, **, {7} | Threshold: 0"
  val ECG_MULTI_FIXED_GAP        = "PUT YOUR QUERY HERE"
  val ECG_MULTI_RANGE_GAP        = "PUT YOUR QUERY HERE"
  val ECG_MULTI_ORDERED_FLEX     = "PUT YOUR QUERY HERE"
  val ECG_MULTI_UNORDERED_FLEX   = "PUT YOUR QUERY HERE"

  // TSBS subsequence query case
  val TSBS_FREE_FIXED_LEN = "PUT YOUR QUERY HERE"

  // RANDOMWALK subsequence query case
  val RANDOMWALK_FREE_FIXED_LEN = "PUT YOUR QUERY HERE"


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


  // PATTERN RETRIEVAL

  def getPattern(dataset: String = DATASET, matchingType: String = MATCHING_TYPE): String = {
    matchingType match {
      case "WHOLE_SEQUENCE" =>
        dataset match {
          case "TSBS" => TSBS_PATTERN
          case "ECG" => ECG_PATTERN
          case "RANDOMWALK" => RANDOMWALK_PATTERN
          case _ => throw new IllegalArgumentException(s"Unknown dataset: $dataset")
        }
      case "SUBSEQUENCE" =>
        getSubsequencePattern(dataset, SUBSEQUENCE_CASE)
      case _ => throw new IllegalArgumentException(s"Unknown matching type: $matchingType")
    }
  }

  def getSubsequencePattern(dataset: String = DATASET, subCase: String = SUBSEQUENCE_CASE): String = {
    (dataset, subCase) match {
      case ("ECG", "FREE_FIXED_LEN")          => ECG_FREE_FIXED_LEN
      case ("ECG", "FREE_DYN_LEN")            => ECG_FREE_DYN_LEN
      case ("ECG", "FREE_DYN_LEN_WIDE")       => ECG_FREE_DYN_LEN_WIDE
      case ("ECG", "BOUNDED_FIXED_LEN")       => ECG_BOUNDED_FIXED_LEN
      case ("ECG", "BOUNDED_DYN_LEN")         => ECG_BOUNDED_DYN_LEN
      case ("ECG", "FIXED_START_FIXED_LEN")   => ECG_FIXED_START_FIXED_LEN
      case ("ECG", "FIXED_START_DYN_LEN")     => ECG_FIXED_START_DYN_LEN
      case ("ECG", "MULTI_ADJACENT")          => ECG_MULTI_ADJACENT
      case ("ECG", "MULTI_FIXED_GAP")         => ECG_MULTI_FIXED_GAP
      case ("ECG", "MULTI_RANGE_GAP")         => ECG_MULTI_RANGE_GAP
      case ("ECG", "MULTI_ORDERED_FLEX_GAP")  => ECG_MULTI_ORDERED_FLEX
      case ("ECG", "MULTI_UNORDERED_FLEX_GAP") => ECG_MULTI_UNORDERED_FLEX

      case ("TSBS", "FREE_FIXED_LEN")       => TSBS_FREE_FIXED_LEN
      case ("RANDOMWALK", "FREE_FIXED_LEN") => RANDOMWALK_FREE_FIXED_LEN

      case _ => throw new IllegalArgumentException(s"Unknown dataset: $dataset or subsequence case: $subCase")
    }
  }


  // GROUND TRUTH SEEDS - Optional accuracy check for subsequence matching.
  // If you plant known matching series in your dataset, list their ids here and
  // the refinement stage will report how many were found. Leave empty to skip.

  def getGroundTruthSeeds(dataset: String = DATASET, datasetSize: String = DATASET_SIZE): Set[Long] =
    Set.empty[Long]

}
