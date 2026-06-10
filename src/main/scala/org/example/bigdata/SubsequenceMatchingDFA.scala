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

object SubsequenceMatchingDFA {

  @transient private lazy val logger = LoggerFactory.getLogger(SubsequenceMatchingDFA.getClass)


  val DATASET = Config.DATASET
  val DATASET_SIZE = Config.DATASET_SIZE
  val ALPHA = Config.ALPHA

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


  sealed trait ValueRange extends Serializable {
    def contains(value: Double): Boolean
  }

  case class InclusiveRange(min: Double, max: Double) extends ValueRange {
    def contains(value: Double): Boolean = value >= min && value <= max
  }

  case class ExclusiveBothRange(min: Double, max: Double) extends ValueRange {
    def contains(value: Double): Boolean = value > min && value < max
  }

  case class LeftInclusiveRightExclusiveRange(min: Double, max: Double) extends ValueRange {
    def contains(value: Double): Boolean = value >= min && value < max
  }

  case class LeftExclusiveRightInclusiveRange(min: Double, max: Double) extends ValueRange {
    def contains(value: Double): Boolean = value > min && value <= max
  }

  case class GreaterThanRange(min: Double, inclusive: Boolean) extends ValueRange {
    def contains(value: Double): Boolean = if (inclusive) value >= min else value > min
  }

  case class LessThanRange(max: Double, inclusive: Boolean) extends ValueRange {
    def contains(value: Double): Boolean = if (inclusive) value <= max else value < max
  }

  sealed trait Direction extends Serializable
  case object Increasing extends Direction
  case object Decreasing extends Direction
  case object Flat extends Direction
  case object Any extends Direction

  sealed trait StepSize extends Serializable
  case class FixedStep(value: Double) extends StepSize
  case class RangeStep(min: Double, max: Double) extends StepSize
  case class FactorStep(multiplier: Double) extends StepSize
  case object NoStep extends StepSize

  sealed trait Threshold extends Serializable {
    def getAbsoluteTolerance(baseValue: Double): Double
  }

  case class AbsoluteThreshold(value: Double) extends Threshold {
    def getAbsoluteTolerance(baseValue: Double): Double = value
  }

  case class PercentageThreshold(percentage: Double) extends Threshold {
    def getAbsoluteTolerance(baseValue: Double): Double = math.abs(baseValue) * (percentage / 100.0)
  }

  case class Pattern(
    valueRange: ValueRange,
    direction: Direction,
    stepSize: StepSize,
    minLength: Int,
    maxLength: Int
  ) extends Serializable

  case class Match(
    timeSeriesId: Long,
    startPosition: Int,
    endPosition: Int,
    length: Int
  ) extends Serializable

  case class MultiPattern(
    patterns: Array[Pattern],
    thresholds: Array[Threshold],
    separatorType: String,
    positions: Option[Array[Int]],
    gapsMinMax: Option[Array[(Int, Int)]],
    singleStartRange: Option[(Int, Int)] = None
  ) extends Serializable {
    def gaps: Option[Array[Int]] = gapsMinMax.map(_.map(_._1))
  }

  case class MultiPatternMatch(
    timeSeriesId: Long,
    patternMatches: Array[Match]
  ) extends Serializable


  private def smartSplit(str: String): Array[String] = {
    val result = scala.collection.mutable.ArrayBuffer[String]()
    var current = new StringBuilder()
    var depth = 0

    str.foreach { char =>
      char match {
        case '[' | '(' | '{' =>
          depth += 1
          current.append(char)
        case ']' | ')' | '}' =>
          depth -= 1
          current.append(char)
        case ',' if depth == 0 =>
          result += current.toString.trim
          current = new StringBuilder()
        case _ =>
          current.append(char)
      }
    }

    if (current.nonEmpty) {
      result += current.toString.trim
    }

    result.toArray
  }

  def parseQuery(queryString: String): (Pattern, Threshold) = {
    val patternRegex = """\|\s*([^\|]+)\s*\|""".r
    val patternStr = patternRegex.findFirstMatchIn(queryString) match {
      case Some(m) => m.group(1).trim
      case None => throw new IllegalArgumentException(s"Invalid query format: $queryString")
    }

    val parts = smartSplit(patternStr)
    if (parts.length != 4) {
      throw new IllegalArgumentException(s"Pattern must have 4 elements, got ${parts.length}: $patternStr")
    }

    val valueRange = parseValueRange(parts(0))

    val direction = parseDirection(parts(1))

    val stepSize = parseStepSize(parts(2), direction)

    val (minLength, maxLength) = parseLength(parts(3))

    val threshold = parseThreshold(queryString, direction, stepSize)

    val pattern = Pattern(valueRange, direction, stepSize, minLength, maxLength)
    (pattern, threshold)
  }

  def parseAllPatterns(queryString: String): MultiPattern = {
    val unorderedPrefix = """(?i)^UNORDERED:\s*""".r
    val unordered = unorderedPrefix.findFirstIn(queryString).isDefined
    val cleaned = unorderedPrefix.replaceFirstIn(queryString, "")

    val patternRegex = """\|\s*([^\|]+)\s*\|""".r
    val patternBlocks = patternRegex.findAllMatchIn(cleaned).map(_.group(1).trim).toArray

    if (patternBlocks.isEmpty) {
      throw new IllegalArgumentException(s"No patterns found in query: $queryString")
    }

    val patternsAndThresholds = patternBlocks.map { block =>
      val parts = smartSplit(block)
      if (parts.length != 4) {
        throw new IllegalArgumentException(s"Pattern must have 4 elements, got ${parts.length}: $block")
      }
      val valueRange = parseValueRange(parts(0))
      val direction = parseDirection(parts(1))
      val stepSize = parseStepSize(parts(2), direction)
      val (minLength, maxLength) = parseLength(parts(3))
      val threshold = parseThreshold(cleaned, direction, stepSize)
      (Pattern(valueRange, direction, stepSize, minLength, maxLength), threshold)
    }

    val patterns = patternsAndThresholds.map(_._1)
    val thresholds = patternsAndThresholds.map(_._2)
    val numPairs = patterns.length - 1

    if (patterns.length == 1) {
      val noThreshold = cleaned.replaceAll("Threshold:\\s*\\S+", "").trim
      val firstMatch = patternRegex.findFirstMatchIn(noThreshold).get
      val afterBlock = noThreshold.substring(firstMatch.end).trim
      val boundedRegex = """^@\[\s*(\d+)\s*,\s*(\d+)\s*\]""".r
      val fixedRegex = """^@(\d+)""".r

      afterBlock match {
        case boundedRegex(start, end) =>
          return MultiPattern(patterns, thresholds, "SINGLE_BOUNDED_RANGE", None, None, Some((start.toInt, end.toInt)))
        case fixedRegex(start) =>
          val s = start.toInt
          return MultiPattern(patterns, thresholds, "SINGLE_FIXED_START", Some(Array(s)), None, Some((s, s)))
        case _ =>
          return MultiPattern(patterns, thresholds, "SINGLE", None, None, None)
      }
    }

    val fixedPosRegex = """@(\d+)(?!\s*-|\s*,\s*\d+\s*\])""".r
    val hasFixedPos = fixedPosRegex.findAllMatchIn(cleaned).toList
    val gapRegex = """#(\d+)(?:-(\d+))?#""".r
    val gapMatches = gapRegex.findAllMatchIn(cleaned).toList
    val hasFlexSeparator = cleaned.contains("....")

    if (hasFixedPos.size >= patterns.length) {
      val positions = hasFixedPos.take(patterns.length).map(_.group(1).toInt).toArray
      return MultiPattern(patterns, thresholds, "FIXED_POS", Some(positions), None)
    }

    if (hasFlexSeparator) {
      val sepType = if (unordered) "UNORDERED_FLEX_GAP" else "ORDERED_FLEX_GAP"
      val flexGaps = Array.fill(numPairs)((0, Int.MaxValue))
      return MultiPattern(patterns, thresholds, sepType, None, Some(flexGaps))
    }

    if (gapMatches.nonEmpty) {
      val gaps: Array[(Int, Int)] = gapMatches.map { m =>
        val min = m.group(1).toInt
        val max = Option(m.group(2)).map(_.toInt).getOrElse(min)
        (min, max)
      }.toArray

      val allAdjacent = gaps.forall { case (a, b) => a == 0 && b == 0 }
      val allFixed    = gaps.forall { case (a, b) => a == b && !(a == 0 && b == 0) }
      val sepType =
        if (allAdjacent) "ADJACENT"
        else if (allFixed) "FIXED_GAP"
        else "RANGE_GAP"
      return MultiPattern(patterns, thresholds, sepType, None, Some(gaps))
    }

    val zeroGaps = Array.fill(numPairs)((0, 0))
    MultiPattern(patterns, thresholds, "ADJACENT", None, Some(zeroGaps))
  }

  def parseValueRange(rangeStr: String): ValueRange = {
    val inclusiveBoth = """\[([0-9.\-]+),\s*([0-9.\-]+)\]""".r
    val exclusiveBoth = """\(([0-9.\-]+),\s*([0-9.\-]+)\)""".r
    val leftIncRightExc = """\[([0-9.\-]+),\s*([0-9.\-]+)\)""".r
    val leftExcRightInc = """\(([0-9.\-]+),\s*([0-9.\-]+)\]""".r
    val greaterThanInc = """\[([0-9.\-]+),\s*\)""".r
    val greaterThanExc = """\(([0-9.\-]+),\s*\)""".r
    val lessThanInc = """\(\s*,\s*([0-9.\-]+)\]""".r
    val lessThanExc = """\(\s*,\s*([0-9.\-]+)\)""".r

    rangeStr match {
      case inclusiveBoth(min, max) => InclusiveRange(min.toDouble, max.toDouble)
      case exclusiveBoth(min, max) => ExclusiveBothRange(min.toDouble, max.toDouble)
      case leftIncRightExc(min, max) => LeftInclusiveRightExclusiveRange(min.toDouble, max.toDouble)
      case leftExcRightInc(min, max) => LeftExclusiveRightInclusiveRange(min.toDouble, max.toDouble)
      case greaterThanInc(min) => GreaterThanRange(min.toDouble, inclusive = true)
      case greaterThanExc(min) => GreaterThanRange(min.toDouble, inclusive = false)
      case lessThanInc(max) => LessThanRange(max.toDouble, inclusive = true)
      case lessThanExc(max) => LessThanRange(max.toDouble, inclusive = false)
      case _ => throw new IllegalArgumentException(s"Invalid value range format: $rangeStr")
    }
  }

  def parseDirection(dirStr: String): Direction = {
    dirStr match {
      case "<+>" => Increasing
      case "<->" => Decreasing
      case "<=>" => Flat
      case "<~>" | "<*>" => Any
      case _ => throw new IllegalArgumentException(s"Invalid direction format: $dirStr (expected <+>, <->, <=>, <~>, or <*>)")
    }
  }

  def parseStepSize(stepStr: String, direction: Direction): StepSize = {
    val trimmed = stepStr.trim()
    if (trimmed.isEmpty || trimmed == "**" || trimmed == "*") {
      NoStep
    } else if (trimmed.startsWith("*") && trimmed.endsWith("*") && trimmed.length > 2) {
      val inner = trimmed.substring(1, trimmed.length - 1)
      FixedStep(inner.toDouble)
    } else {
      if (stepStr.toLowerCase.endsWith("x")) {
        val multiplier = stepStr.dropRight(1).toDouble
        FactorStep(multiplier)
      } else if (stepStr.startsWith("(") && stepStr.endsWith(")")) {
        val rangeRegex = """\(([0-9.]+),\s*([0-9.]+)\)""".r
        stepStr match {
          case rangeRegex(min, max) => RangeStep(min.toDouble, max.toDouble)
          case _ => throw new IllegalArgumentException(s"Invalid step range format: $stepStr")
        }
      } else {
        FixedStep(stepStr.toDouble)
      }
    }
  }

  def parseLength(lengthStr: String): (Int, Int) = {
    val fixed = """\{(\d+)\}""".r
    val range = """\{(\d+),\s*(\d+)\}""".r

    lengthStr match {
      case fixed(n) => (n.toInt, n.toInt)
      case range(min, max) => (min.toInt, max.toInt)
      case _ => throw new IllegalArgumentException(s"Invalid length format: $lengthStr")
    }
  }

  def parseThreshold(queryString: String, direction: Direction, stepSize: StepSize): Threshold = {
    val thresholdRegex = """Threshold:\s*(\d+\.?\d*)(%?)""".r
    thresholdRegex.findFirstMatchIn(queryString) match {
      case Some(m) =>
        val value = m.group(1).toDouble
        val isPercentage = m.group(2) == "%"
        if (isPercentage) PercentageThreshold(value)
        else AbsoluteThreshold(value)

      case None =>
        (direction, stepSize) match {
          case (Flat, _) => AbsoluteThreshold(0.1)
          case (Any, _) => AbsoluteThreshold(0.0)
          case (_, FixedStep(_)) => PercentageThreshold(15.0)
          case (_, RangeStep(_, _)) => PercentageThreshold(10.0)
          case (_, FactorStep(_)) => PercentageThreshold(10.0)
          case (_, NoStep) => PercentageThreshold(15.0)
        }
    }
  }


  class StreamingDFA(pattern: Pattern, threshold: Threshold) extends Serializable {
    private var state: Int = 0
    private var matchStartPos: Int = -1
    private var lastValue: Double = Double.NaN
    private val matches = ArrayBuffer[Match]()

    def feedPoint(position: Int, value: Double, timeSeriesId: Long): Unit = {
      state match {
        case 0 =>
          if (pattern.valueRange.contains(value)) {
            state = 1
            matchStartPos = position
            lastValue = value
          }

        case k if k >= 1 && k < pattern.minLength =>
          if (continuesPattern(value)) {
            state = k + 1
            lastValue = value

            if (state == pattern.minLength && pattern.minLength == pattern.maxLength) {
              matches += Match(timeSeriesId, matchStartPos, position, state)
              reset()
            }
          } else {
            reset()
            feedPoint(position, value, timeSeriesId)
          }

        case k if k >= pattern.minLength && k < pattern.maxLength =>
          if (continuesPattern(value)) {
            state = k + 1
            lastValue = value

            if (state == pattern.maxLength) {
              matches += Match(timeSeriesId, matchStartPos, position, state)
              reset()
            }
          } else {
            matches += Match(timeSeriesId, matchStartPos, position - 1, state)
            reset()
            feedPoint(position, value, timeSeriesId)
          }

        case _ =>
          reset()
          feedPoint(position, value, timeSeriesId)
      }
    }

    private def continuesPattern(currentValue: Double): Boolean = {
      if (!pattern.valueRange.contains(currentValue)) return false

      val actualChange = currentValue - lastValue

      pattern.direction match {
        case Flat =>
          val tolerance = threshold.getAbsoluteTolerance(lastValue)
          if (math.abs(actualChange) > tolerance) return false

        case Increasing =>
          if (actualChange <= 0) return false

        case Decreasing =>
          if (actualChange >= 0) return false

        case Any =>
      }

      pattern.stepSize match {
        case NoStep =>
          true

        case FixedStep(step) =>
          val expectedChange = pattern.direction match {
            case Increasing => step
            case Decreasing => -step
            case Flat => 0.0
            case Any => throw new IllegalStateException("ANY direction cannot have fixed step")
          }
          val tolerance = threshold.getAbsoluteTolerance(step)
          math.abs(actualChange - expectedChange) <= tolerance

        case RangeStep(minStep, maxStep) =>
          val actualMagnitude = math.abs(actualChange)
          val midpoint = (minStep + maxStep) / 2.0
          val tolerance = threshold.getAbsoluteTolerance(midpoint)
          actualMagnitude >= (minStep - tolerance) && actualMagnitude <= (maxStep + tolerance)

        case FactorStep(multiplier) =>
          val expectedValue = lastValue * multiplier
          val tolerance = threshold.getAbsoluteTolerance(expectedValue)
          math.abs(currentValue - expectedValue) <= tolerance
      }
    }

    def flush(timeSeriesId: Long): Unit = {
      if (state >= pattern.minLength && state < pattern.maxLength) {
        matches += Match(timeSeriesId, matchStartPos, matchStartPos + state - 1, state)
        reset()
      }
    }

    def getMatches: Array[Match] = matches.toArray

    private def reset(): Unit = {
      state = 0
      matchStartPos = -1
      lastValue = Double.NaN
    }
  }


  def findMultiPatternMatches(
    tsId: Long,
    tsString: String,
    multiPattern: MultiPattern
  ): Array[MultiPatternMatch] = {

    val values = parseTimeSeries(tsString)
    if (values.isEmpty) return Array.empty[MultiPatternMatch]

    if (multiPattern.patterns.length == 1) {
      val matches = findMatchesWithStreamingDFA(tsId, tsString, multiPattern.patterns(0), multiPattern.thresholds(0))
      return filterSinglePatternMatches(matches, multiPattern).map(m => MultiPatternMatch(tsId, Array(m)))
    }

    val matchesPerPattern: Array[Array[Match]] = multiPattern.patterns.zipWithIndex.map { case (pattern, idx) =>
      findMatchesInValues(tsId, values, pattern, multiPattern.thresholds(idx))
    }

    if (matchesPerPattern.exists(_.isEmpty)) {
      return Array.empty[MultiPatternMatch]
    }

    multiPattern.separatorType match {
      case "FIXED_POS" =>
        val positions = multiPattern.positions.getOrElse(Array.empty[Int])
        val validMatches = new Array[Match](multiPattern.patterns.length)
        var allFound = true

        for (i <- multiPattern.patterns.indices) {
          if (i < positions.length) {
            val requiredStart = positions(i)
            val matchAtPos = matchesPerPattern(i).find(_.startPosition == requiredStart)
            matchAtPos match {
              case Some(m) => validMatches(i) = m
              case None => allFound = false
            }
          } else {
            validMatches(i) = matchesPerPattern(i).head
          }
        }

        if (allFound) Array(MultiPatternMatch(tsId, validMatches))
        else Array.empty[MultiPatternMatch]

      case "ADJACENT" | "FIXED_GAP" | "RANGE_GAP" | "ORDERED_FLEX_GAP" =>
        val gaps = multiPattern.gapsMinMax.getOrElse(Array.fill(multiPattern.patterns.length - 1)((0, 0)))
        findOrderedGapChain(tsId, matchesPerPattern, gaps)

      case "UNORDERED_FLEX_GAP" =>
        findUnorderedMatches(tsId, matchesPerPattern)

      case "GAP" =>
        val gaps = multiPattern.gaps.getOrElse(Array.empty[Int])
        val gapsMinMax = gaps.map(g => (g, g))
        findOrderedGapChain(tsId, matchesPerPattern, gapsMinMax)
      case "INDEPENDENT" =>
        findUnorderedMatches(tsId, matchesPerPattern)

      case _ =>
        val selectedMatches = matchesPerPattern.map(_.head)
        Array(MultiPatternMatch(tsId, selectedMatches))
    }
  }

  private def findOrderedGapChain(
    tsId: Long,
    matchesPerPattern: Array[Array[Match]],
    gaps: Array[(Int, Int)]
  ): Array[MultiPatternMatch] = {
    val results = ArrayBuffer[MultiPatternMatch]()
    val numPatterns = matchesPerPattern.length

    def extend(patternIdx: Int, chain: ArrayBuffer[Match]): Unit = {
      if (patternIdx == numPatterns) {
        results += MultiPatternMatch(tsId, chain.toArray)
      } else {
        val prevMatch = chain.last
        val gapIdx = math.min(patternIdx - 1, gaps.length - 1)
        val (minGap, maxGap) = if (gapIdx >= 0) gaps(gapIdx) else (0, 0)
        val minStart = prevMatch.endPosition + minGap + 1
        val maxStart = if (maxGap == Int.MaxValue) Int.MaxValue else prevMatch.endPosition + maxGap + 1

        matchesPerPattern(patternIdx)
          .filter(m => m.startPosition >= minStart && m.startPosition <= maxStart)
          .foreach { next =>
            chain += next
            extend(patternIdx + 1, chain)
            chain.remove(chain.length - 1)
          }
      }
    }

    for (firstMatch <- matchesPerPattern(0)) {
      extend(1, ArrayBuffer[Match](firstMatch))
    }

    results.toArray
  }

  private def findUnorderedMatches(
    tsId: Long,
    matchesPerPattern: Array[Array[Match]]
  ): Array[MultiPatternMatch] = {
    val numPatterns = matchesPerPattern.length

    val permutations = matchesPerPattern.indices.toList.permutations.toList

    val results = ArrayBuffer[MultiPatternMatch]()
    val seen = scala.collection.mutable.Set[String]()

    for (perm <- permutations) {
      def extend(orderPos: Int, prevEnd: Int, picked: Array[Match]): Unit = {
        if (orderPos == perm.length) {
          val key = picked.map(m => s"${m.startPosition}:${m.endPosition}").mkString("|")
          if (!seen.contains(key)) {
            seen += key
            results += MultiPatternMatch(tsId, picked.clone())
          }
        } else {
          val idx = perm(orderPos)
          matchesPerPattern(idx)
            .filter(_.startPosition > prevEnd)
            .foreach { m =>
              picked(idx) = m
              extend(orderPos + 1, m.endPosition, picked)
              picked(idx) = null
            }
        }
      }
      extend(0, -1, new Array[Match](numPatterns))
    }

    results.toArray
  }


  def main(args: Array[String]): Unit = {
    val queryString = if (args.length > 0) args(0) else {
      "| [10, 50], <->, 1.00, {32} | Threshold: 15%"
    }

    println("\n" + "=" * 80)
    println(s"SUBSEQUENCE MATCHING WITH STREAMING DFA")
    println("=" * 80)
    println(s"Query: $queryString")

    val overallStartTime = System.nanoTime()

    val (pattern, threshold) = parseQuery(queryString)
    println(s"Pattern: $pattern")
    println(s"Threshold: $threshold")

    val conf = new SparkConf()
      .setAppName(s"SubsequenceMatching_${DATASET}_${DATASET_SIZE}")
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
      println(s"\nDataset: $DATASET $DATASET_SIZE")
      println(s"Alpha: $ALPHA")
      println("=" * 80)

      val alphaStr = f"$ALPHA%.2f"
      val queryResultsPath = DATASET match {
        case "TSBS" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/tsbs/query_results_alpha_${alphaStr}"
        case "ECG" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/ecg/query_results_alpha_${alphaStr}"
        case "RANDOMWALK" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/random_walk/query_results_alpha_${alphaStr}"
        case _ => throw new IllegalArgumentException(s"Unknown dataset: $DATASET")
      }

      println(s"\nReading query results from: $queryResultsPath")
      println(s"Processing $NUM_TABLES tables")

      val tableTaskPairs = (1 to NUM_TABLES).map { tableNum =>
        val fileName = s"query_results_table${tableNum}.csv"
        val filePath = s"$queryResultsPath/$fileName"
        val machine = tableToMachine(tableNum)
        val pgTableName = s"${TABLE_PREFIX}_table${tableNum}"
        (tableNum, filePath, machine, pgTableName)
      }

      val patternBroadcast = spark.sparkContext.broadcast(pattern)
      val thresholdBroadcast = spark.sparkContext.broadcast(threshold)

      val hadoopConfBroadcast = spark.sparkContext.broadcast(
        new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
      )

      println("\n" + "=" * 80)
      println("PARALLEL RETRIEVAL + STREAMING DFA REFINEMENT")
      println("=" * 80)

      val results = spark.sparkContext
        .parallelize(tableTaskPairs, tableTaskPairs.size)
        .mapPartitions { partitionIter =>
          val hadoopConf = hadoopConfBroadcast.value.value
          val fs = FileSystem.newInstance(hadoopConf)
          val localPattern = patternBroadcast.value
          val localThreshold = thresholdBroadcast.value

          try {
            partitionIter.map { case (tableNum, queryFilePath, machine, pgTableName) =>
              processTable(fs, tableNum, queryFilePath, machine, pgTableName, localPattern, localThreshold)
            }.toList.iterator
          } finally {
            fs.close()
          }
        }
        .collect()
        .sortBy(_._1)

      println("\n" + "=" * 80)
      println("AGGREGATING RESULTS")
      println("=" * 80)

      val totalCandidates = results.map(_._2).sum
      val totalMatches = results.map(_._3).sum
      val allMatches = results.flatMap(_._4)

      println(f"Total candidates retrieved: $totalCandidates%,d")
      println(f"Total subsequence matches found: $totalMatches%,d")

      if (totalCandidates > 0) {
        val matchRate = totalMatches.toDouble / totalCandidates * 100.0
        println(f"Match rate: $matchRate%.2f%%")
      }

      val matchesByTS = allMatches.groupBy(_.timeSeriesId)
      println(f"\nUnique time series with matches: ${matchesByTS.size}%,d")

      if (allMatches.nonEmpty) {
        println("\n" + "=" * 80)
        println("SAVING RESULTS TO HDFS")
        println("=" * 80)

        val timestamp = java.time.LocalDateTime.now()
          .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
        val outputPath = DATASET match {
          case "TSBS" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/tsbs/subsequence_matches_alpha_${alphaStr}_${timestamp}"
          case "ECG" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/ecg/subsequence_matches_alpha_${alphaStr}_${timestamp}"
          case "RANDOMWALK" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/random_walk/subsequence_matches_alpha_${alphaStr}_${timestamp}"
          case _ => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/subsequence_matches_alpha_${alphaStr}_${timestamp}"
        }

        println(s"Saving ${allMatches.length} matches to: $outputPath")

        val matchesRDD = spark.sparkContext.parallelize(allMatches.sortBy(m => (m.timeSeriesId, m.startPosition)))
          .map { m => s"${m.timeSeriesId},${m.startPosition},${m.endPosition},${m.length}" }

        val header = spark.sparkContext.parallelize(Seq("time_series_id,start_position,end_position,length"))
        header.union(matchesRDD).saveAsTextFile(outputPath)

        println(s"✓ Results saved to HDFS: $outputPath")
      } else {
        println("\nNo matches found, skipping HDFS save.")
      }

      val overallEndTime = System.nanoTime()
      val totalDuration = (overallEndTime - overallStartTime) / 1e9d / 60.0

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
              println(s"[Candidates] Skipping invalid value '$trimmed': ${e.getMessage}")
          }
        }
        line = reader.readLine()
      }
    } finally {
      if (reader != null) reader.close()
    }

    buffer.distinct.toArray
  }

  def processTable(
    fs: FileSystem,
    tableNum: Int,
    queryFilePath: String,
    machine: String,
    pgTableName: String,
    pattern: Pattern,
    threshold: Threshold
  ): (Int, Long, Long, Array[Match]) = {

    val startTime = System.nanoTime()
    println(s"\n[Table $tableNum on $machine] Starting processing...")

    try {
      val candidateIds = loadCandidateIds(fs, queryFilePath)
      println(s"[Table $tableNum] Found ${candidateIds.length} candidates")

      if (candidateIds.isEmpty) {
        println(s"[Table $tableNum] No candidates, skipping...")
        return (tableNum, 0L, 0L, Array.empty[Match])
      }

      val matches = retrieveAndRefineWithStreamingDFA(
        machine, pgTableName, candidateIds, pattern, threshold
      )

      val endTime = System.nanoTime()
      val totalTime = (endTime - startTime) / 1e9d
      println(f"[Table $tableNum] ✓ Complete in $totalTime%.2f seconds - ${matches.length} matches")

      (tableNum, candidateIds.length.toLong, matches.length.toLong, matches)

    } catch {
      case e: Exception =>
        println(s"[Table $tableNum] ✗ ERROR: ${e.getMessage}")
        e.printStackTrace()
        (tableNum, 0L, 0L, Array.empty[Match])
    }
  }

  def retrieveAndRefineWithStreamingDFA(
    machine: String,
    tableName: String,
    candidateIds: Array[Long],
    pattern: Pattern,
    threshold: Threshold
  ): Array[Match] = {

    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    val allMatches = ArrayBuffer[Match]()

    val batches = candidateIds.grouped(BATCH_SIZE)
    println(s"  Processing ${candidateIds.length / BATCH_SIZE + 1} batches...")

    var conn: Connection = null
    var stmt: java.sql.PreparedStatement = null

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      stmt = conn.prepareStatement(s"SELECT ts_id, ts_data FROM $tableName WHERE ts_id = ANY (?)")

      var batchIdx = 0
      batches.foreach { batch =>
        val sqlArray = conn.createArrayOf("BIGINT", batch.map(Long.box))
        stmt.setArray(1, sqlArray)
        val rs = stmt.executeQuery()

        while (rs.next()) {
          val tsId = rs.getLong("ts_id")
          val tsData = rs.getString("ts_data")

          val matches = findMatchesWithStreamingDFA(tsId, tsData, pattern, threshold)
          allMatches ++= matches
        }

        rs.close()
        sqlArray.free()

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

    allMatches.toArray
  }

  def findMatchesWithStreamingDFA(
    tsId: Long,
    tsString: String,
    pattern: Pattern,
    threshold: Threshold
  ): Array[Match] = {

    val values = parseTimeSeries(tsString)
    if (values.isEmpty) return Array.empty[Match]

    findMatchesInValues(tsId, values, pattern, threshold)
  }

  private def findMatchesInValues(
    tsId: Long,
    values: Array[Double],
    pattern: Pattern,
    threshold: Threshold
  ): Array[Match] = {
    if (canUseFastPath(pattern, threshold)) {
      return findMatchesInValuesFast(tsId, values, pattern, threshold.asInstanceOf[AbsoluteThreshold])
    }
    findMatchesInValuesGeneric(tsId, values, pattern, threshold)
  }

  private def canUseFastPath(pattern: Pattern, threshold: Threshold): Boolean = {
    val dirOk = pattern.direction == Increasing || pattern.direction == Decreasing
    val stepOk = pattern.stepSize == NoStep
    val thrOk = threshold match {
      case AbsoluteThreshold(0.0) => true
      case _                      => false
    }
    val rangeOk = pattern.valueRange match {
      case _: InclusiveRange | _: ExclusiveBothRange |
           _: LeftInclusiveRightExclusiveRange | _: LeftExclusiveRightInclusiveRange |
           _: GreaterThanRange | _: LessThanRange => true
    }
    dirOk && stepOk && thrOk && rangeOk
  }

  private def findMatchesInValuesFast(
    tsId: Long,
    values: Array[Double],
    pattern: Pattern,
    threshold: AbsoluteThreshold
  ): Array[Match] = {
    val n = values.length
    if (n == 0) return Array.empty[Match]

    val (yLo, yHi, lowInc, highInc) = pattern.valueRange match {
      case InclusiveRange(min, max)                   => (min, max, true,  true)
      case ExclusiveBothRange(min, max)               => (min, max, false, false)
      case LeftInclusiveRightExclusiveRange(min, max) => (min, max, true,  false)
      case LeftExclusiveRightInclusiveRange(min, max) => (min, max, false, true)
      case GreaterThanRange(min, inclusive)           => (min, Double.PositiveInfinity, inclusive, true)
      case LessThanRange(max, inclusive)              => (Double.NegativeInfinity, max, true, inclusive)
    }

    val prefixInRange = new Array[Int](n)
    val nMono = n - 1
    val prefixMono = if (nMono > 0) new Array[Int](nMono) else Array.empty[Int]
    val isInc = pattern.direction == Increasing

    var sumIR = 0
    var sumMono = 0
    var i = 0
    while (i < n) {
      val v = values(i)
      val lowOk  = if (lowInc)  v >= yLo else v > yLo
      val highOk = if (highInc) v <= yHi else v < yHi
      if (lowOk && highOk) sumIR += 1
      prefixInRange(i) = sumIR
      if (i < nMono) {
        val next = values(i + 1)
        val moOk = if (isInc) next > v else next < v
        if (moOk) sumMono += 1
        prefixMono(i) = sumMono
      }
      i += 1
    }

    val matches = ArrayBuffer[Match]()
    val minLen = pattern.minLength
    val maxLen = pattern.maxLength
    var start = 0
    while (start < n) {
      val maxLenHere = math.min(maxLen, n - start)
      if (maxLenHere >= minLen) {
        var len = minLen
        while (len <= maxLenHere) {
          val irPrev   = if (start > 0) prefixInRange(start - 1) else 0
          val irCount  = prefixInRange(start + len - 1) - irPrev
          val rangeOk  = irCount == len
          val monoOk   = if (len < 2) true else {
            val monoEndIdx = start + len - 2
            val monoPrev   = if (start > 0) prefixMono(start - 1) else 0
            val monoCount  = prefixMono(monoEndIdx) - monoPrev
            monoCount == (len - 1)
          }
          if (rangeOk && monoOk) matches += Match(tsId, start, start + len - 1, len)
          len += 1
        }
      }
      start += 1
    }
    matches.toArray
  }

  private def findMatchesInValuesGeneric(
    tsId: Long,
    values: Array[Double],
    pattern: Pattern,
    threshold: Threshold
  ): Array[Match] = {
    val matches = ArrayBuffer[Match]()
    if (values.isEmpty) return matches.toArray

    for (start <- values.indices) {
      val maxLenHere = math.min(pattern.maxLength, values.length - start)
      for (len <- pattern.minLength to maxLenHere) {
        if (windowMatches(values, start, len, pattern, threshold)) {
          matches += Match(tsId, start, start + len - 1, len)
        }
      }
    }

    matches.toArray
  }

  private def filterSinglePatternMatches(
    matches: Array[Match],
    multiPattern: MultiPattern
  ): Array[Match] = {
    multiPattern.singleStartRange match {
      case Some((start, end)) => matches.filter(m => m.startPosition >= start && m.startPosition <= end)
      case None => matches
    }
  }

  private def windowMatches(
    values: Array[Double],
    start: Int,
    length: Int,
    pattern: Pattern,
    threshold: Threshold
  ): Boolean = {
    if (length <= 0 || start < 0 || start + length > values.length) return false

    var i = 0
    while (i < length) {
      if (!pattern.valueRange.contains(values(start + i))) return false
      i += 1
    }

    i = 1
    while (i < length) {
      val prev = values(start + i - 1)
      val curr = values(start + i)
      if (!transitionMatches(prev, curr, pattern, threshold)) return false
      i += 1
    }

    true
  }

  private def transitionMatches(
    previousValue: Double,
    currentValue: Double,
    pattern: Pattern,
    threshold: Threshold
  ): Boolean = {
    val actualChange = currentValue - previousValue

    pattern.direction match {
      case Flat =>
        val tolerance = threshold.getAbsoluteTolerance(previousValue)
        if (math.abs(actualChange) > tolerance) return false
      case Increasing =>
        if (actualChange <= 0) return false
      case Decreasing =>
        if (actualChange >= 0) return false
      case Any =>
    }

    pattern.stepSize match {
      case NoStep =>
        true
      case FixedStep(step) =>
        val expectedChange = pattern.direction match {
          case Increasing => step
          case Decreasing => -step
          case Flat => 0.0
          case Any => actualChange
        }
        val tolerance = threshold.getAbsoluteTolerance(step)
        math.abs(actualChange - expectedChange) <= tolerance
      case RangeStep(minStep, maxStep) =>
        val actualMagnitude = math.abs(actualChange)
        val midpoint = (minStep + maxStep) / 2.0
        val tolerance = threshold.getAbsoluteTolerance(midpoint)
        actualMagnitude >= (minStep - tolerance) && actualMagnitude <= (maxStep + tolerance)
      case FactorStep(multiplier) =>
        val expectedValue = previousValue * multiplier
        val tolerance = threshold.getAbsoluteTolerance(expectedValue)
        math.abs(currentValue - expectedValue) <= tolerance
    }
  }

  def parseTimeSeries(tsString: String): Array[Double] = parseTimeSeriesFast(tsString)

  def parseTimeSeriesFast(tsString: String): Array[Double] = {
    if (tsString == null || tsString.isEmpty) return Array.empty[Double]
    val n = tsString.length
    var out = new Array[Double](128)
    var outIdx = 0
    var i = 0
    try {
      while (i < n) {
        while (i < n && tsString.charAt(i) != '(') i += 1
        if (i >= n) {
          return if (outIdx == out.length) out else java.util.Arrays.copyOf(out, outIdx)
        }
        i += 1
        while (i < n && tsString.charAt(i) != ' ') i += 1
        if (i >= n) return java.util.Arrays.copyOf(out, outIdx)
        i += 1
        val yStart = i
        while (i < n && tsString.charAt(i) != ')') i += 1
        if (i > yStart) {
          val y = java.lang.Double.parseDouble(tsString.substring(yStart, i))
          if (outIdx >= out.length) {
            val grown = new Array[Double](out.length * 2)
            System.arraycopy(out, 0, grown, 0, out.length)
            out = grown
          }
          out(outIdx) = y
          outIdx += 1
        }
        if (i < n) i += 1
      }
    } catch {
      case e: Exception =>
        logger.error(s"parseTimeSeriesFast error at byte $i (~${tsString.take(50)}...): ${e.getMessage}")
        return Array.empty[Double]
    }
    if (outIdx == out.length) out else java.util.Arrays.copyOf(out, outIdx)
  }
}
