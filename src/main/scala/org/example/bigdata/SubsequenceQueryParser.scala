package org.example.bigdata

object SubsequenceQueryParser {


  sealed trait PositionConstraint extends Serializable
  case class FixedStart(start: Int) extends PositionConstraint
  case class BoundedRange(rangeStart: Int, rangeEnd: Int) extends PositionConstraint
  case object FreePosition extends PositionConstraint

  case class PatternSpec(
    yStart: Double,
    yEnd: Double,
    direction: String,
    stepSize: Double,
    minLength: Int,
    maxLength: Int,
    position: PositionConstraint
  ) {
    def isFixedLength: Boolean = minLength == maxLength
    def isDynamicLength: Boolean = minLength != maxLength
    def yMin: Double = math.min(yStart, yEnd)
    def yMax: Double = math.max(yStart, yEnd)
  }

  case class Gap(minLength: Int, maxLength: Int) {
    def isFixed: Boolean = minLength == maxLength
    def isAdjacent: Boolean = minLength == 0 && maxLength == 0
    def isFlexible: Boolean = maxLength == Int.MaxValue
    def length: Int = minLength
  }

  object Gap {
    def apply(fixed: Int): Gap = Gap(fixed, fixed)
    val Flexible: Gap = Gap(0, Int.MaxValue)
  }

  sealed trait ProbeGeometry extends Serializable
  case class LineProbe(x: Double, yMin: Double, yMax: Double) extends ProbeGeometry
  case class EnvelopeProbe(xMin: Double, yMin: Double, xMax: Double, yMax: Double) extends ProbeGeometry

  case class PatternProbes(
    leftProbe: ProbeGeometry,
    rightProbe: ProbeGeometry,
    pattern: PatternSpec
  )

  sealed trait SubsequenceCase extends Serializable
  case object Case1A extends SubsequenceCase
  case object Case1B extends SubsequenceCase
  case object Case1C extends SubsequenceCase
  case object Case2A extends SubsequenceCase
  case object Case2B extends SubsequenceCase
  case object Case2C extends SubsequenceCase
  case object CaseFreeFixedLen extends SubsequenceCase
  case object CaseFreeDynLen extends SubsequenceCase
  case object CaseBoundedFixedLen extends SubsequenceCase
  case object CaseBoundedDynLen extends SubsequenceCase
  case object CaseFixedStartFixedLen extends SubsequenceCase
  case object CaseFixedStartDynLen extends SubsequenceCase
  case object CaseMultiAdjacent extends SubsequenceCase
  case object CaseMultiFixedGap extends SubsequenceCase
  case object CaseMultiRangeGap extends SubsequenceCase
  case object CaseMultiOrderedFlexGap extends SubsequenceCase
  case object CaseMultiUnorderedFlexGap extends SubsequenceCase

  case class SubsequenceQuery(
    detectedCase: SubsequenceCase,
    patterns: List[PatternSpec],
    gaps: List[Gap],
    globalBound: Option[BoundedRange],
    threshold: Double,
    probes: List[PatternProbes],
    unordered: Boolean = false
  )


  def parse(queryString: String, epsilon: Double): SubsequenceQuery = {
    val threshold = parseThreshold(queryString)

    val unorderedRegex = """(?i)^UNORDERED:\s*""".r
    val unordered = unorderedRegex.findFirstIn(queryString).isDefined
    val stripped = unorderedRegex.replaceFirstIn(queryString, "")

    val baseStr = stripped.replaceAll("Threshold:\\s*\\S+", "").trim

    if (baseStr.contains("....")) {
      val q = parseCase2C(baseStr, epsilon, threshold)
      return q.copy(
        detectedCase = if (unordered) CaseMultiUnorderedFlexGap else CaseMultiOrderedFlexGap,
        unordered = unordered
      )
    }

    if (baseStr.matches("""(?s).*#\d+(-\d+)?#.*""")) {
      return parseCase2B(baseStr, epsilon, threshold)
    }

    val patternBlocks = extractPatternBlocks(baseStr)

    if (patternBlocks.size == 1) {
      val block = patternBlocks.head
      val (spec, _) = parsePatternBlock(block)
      val blockEnd = baseStr.indexOf(block) + block.length
      val posConstraint = extractPositionAfterBlock(baseStr, blockEnd)
      val pattern = spec.copy(position = posConstraint)

      posConstraint match {
        case FixedStart(_) =>
          val probes = generateProbes1A(pattern, epsilon)
          val c = if (pattern.isFixedLength) CaseFixedStartFixedLen else CaseFixedStartDynLen
          SubsequenceQuery(c, List(pattern), Nil, None, threshold, List(probes))
        case BoundedRange(_, _) =>
          val probes = generateProbes1B(pattern, epsilon)
          val c = if (pattern.isFixedLength) CaseBoundedFixedLen else CaseBoundedDynLen
          SubsequenceQuery(c, List(pattern), Nil, None, threshold, List(probes))
        case FreePosition =>
          val c = if (pattern.isFixedLength) CaseFreeFixedLen else CaseFreeDynLen
          SubsequenceQuery(c, List(pattern), Nil, None, threshold, Nil)
      }
    } else {
      val hasAnyPositionAnchor = """@(\d+)(?!\])""".r.findFirstIn(baseStr).isDefined
      if (hasAnyPositionAnchor) {
        parseCase2A(baseStr, patternBlocks, epsilon, threshold)
      } else {
        val patterns = patternBlocks.map { block =>
          val (spec, _) = parsePatternBlock(block)
          spec
        }
        val gaps = List.fill(patterns.length - 1)(Gap(0, 0))
        SubsequenceQuery(CaseMultiAdjacent, patterns, gaps, None, threshold, Nil)
      }
    }
  }


  private def extractPatternBlocks(s: String): List[String] = {
    val regex = """\|[^|]+\|""".r
    regex.findAllIn(s).toList
  }

  private def parsePatternBlock(block: String): (PatternSpec, String) = {
    val inner = block.replaceAll("^\\|\\s*", "").replaceAll("\\s*\\|$", "").trim

    val parts = smartSplit(inner, ',')
    if (parts.length < 4) {
      throw new IllegalArgumentException(s"Pattern block must have at least 4 parts: $block")
    }

    val valueRange = parts(0).trim
    val dirStr = parts(1).trim
    val stepStr = parts(2).trim
    val lengthStr = parts(3).trim

    val (yStart, yEnd) = parseValueRangePair(valueRange)
    val direction = parseDirection(dirStr)
    val stepSize = parseStepSize(stepStr)
    val (minLen, maxLen) = parseLength(lengthStr)

    val spec = PatternSpec(yStart, yEnd, direction, stepSize, minLen, maxLen, FreePosition)
    (spec, "")
  }


  private def parsePositionAnnotation(afterBlock: String): PositionConstraint = {
    val fixedRegex = """@(\d+)""".r
    val boundedRegex = """@\[\s*(\d+)\s*,\s*(\d+)\s*\]""".r

    afterBlock.trim match {
      case boundedRegex(a, b) => BoundedRange(a.toInt, b.toInt)
      case fixedRegex(n) => FixedStart(n.toInt)
      case _ => FreePosition
    }
  }

  private def extractPositionAfterBlock(fullStr: String, blockEnd: Int): PositionConstraint = {
    val after = fullStr.substring(blockEnd).trim
    val boundedRegex = """^@\[\s*(\d+)\s*,\s*(\d+)\s*\]""".r
    val fixedRegex = """^@(\d+)""".r

    after match {
      case boundedRegex(a, b) => BoundedRange(a.toInt, b.toInt)
      case fixedRegex(n) => FixedStart(n.toInt)
      case _ => FreePosition
    }
  }


  private def parseCase2A(baseStr: String, blocks: List[String], epsilon: Double, threshold: Double): SubsequenceQuery = {
    val patterns = scala.collection.mutable.ListBuffer[PatternSpec]()
    var remaining = baseStr

    for (block <- blocks) {
      val blockIdx = remaining.indexOf(block)
      val afterIdx = blockIdx + block.length
      val afterStr = remaining.substring(afterIdx).trim

      val boundedRegex = """^@\[\s*(\d+)\s*,\s*(\d+)\s*\]""".r
      val fixedRegex = """^@(\d+)""".r

      val pos = afterStr match {
        case boundedRegex(a, b) => BoundedRange(a.toInt, b.toInt)
        case fixedRegex(n) => FixedStart(n.toInt)
        case _ => FreePosition
      }

      val (spec, _) = parsePatternBlock(block)
      patterns += spec.copy(position = pos)
      remaining = remaining.substring(afterIdx)
    }

    val rewrittenPatterns = applyOverlapRewriting(patterns.toList)

    val probes = rewrittenPatterns.map(p => generateProbes1A(p, epsilon))
    SubsequenceQuery(Case2A, rewrittenPatterns, Nil, None, threshold, probes)
  }

  private def parseCase2B(baseStr: String, epsilon: Double, threshold: Double): SubsequenceQuery = {
    val globalBoundRegex = """@\[\s*(\d+)\s*,\s*(\d+)\s*\]""".r
    val globalBound: Option[BoundedRange] = globalBoundRegex.findFirstMatchIn(baseStr).map { m =>
      BoundedRange(m.group(1).toInt, m.group(2).toInt)
    }

    val withoutBound = baseStr.replaceAll("""@\[\s*\d+\s*,\s*\d+\s*\]""", "").trim

    val gapRegex = """#(\d+)(?:-(\d+))?#""".r
    val gaps: List[Gap] = gapRegex.findAllMatchIn(withoutBound).map { m =>
      val min = m.group(1).toInt
      val max = Option(m.group(2)).map(_.toInt).getOrElse(min)
      Gap(min, max)
    }.toList

    val blocks = extractPatternBlocks(withoutBound)
    val patterns = blocks.map { block =>
      val (spec, _) = parsePatternBlock(block)
      globalBound match {
        case Some(b) => spec.copy(position = b)
        case None    => spec
      }
    }

    val allAdjacent = gaps.nonEmpty && gaps.forall(_.isAdjacent)
    val allFixed    = gaps.nonEmpty && gaps.forall(g => g.isFixed && !g.isAdjacent)
    val detectedCase: SubsequenceCase =
      if (allAdjacent) CaseMultiAdjacent
      else if (allFixed) CaseMultiFixedGap
      else CaseMultiRangeGap

    SubsequenceQuery(detectedCase, patterns, gaps, globalBound, threshold, Nil)
  }

  private def parseCase2C(baseStr: String, epsilon: Double, threshold: Double): SubsequenceQuery = {
    val parts = baseStr.split("""\.\.\.\.""").map(_.trim).filter(_.nonEmpty)
    val patterns = parts.flatMap { part =>
      val blocks = extractPatternBlocks(part)
      blocks.map { block =>
        val (spec, _) = parsePatternBlock(block)
        spec
      }
    }.toList

    SubsequenceQuery(Case2C, patterns, Nil, None, threshold, Nil)
  }


  def generateProbes1A(pattern: PatternSpec, epsilon: Double): PatternProbes = {
    val start = pattern.position match {
      case FixedStart(s) => s
      case _ => throw new IllegalArgumentException("1A requires FixedStart position")
    }
    val yLow = pattern.yMin - epsilon
    val yHigh = pattern.yMax + epsilon
    val rightX = start + pattern.maxLength - 1

    val leftProbe = LineProbe(start.toDouble, yLow, yHigh)

    if (pattern.isDynamicLength) {
      PatternProbes(leftProbe = leftProbe, rightProbe = leftProbe, pattern = pattern)
    } else {
      PatternProbes(
        leftProbe = leftProbe,
        rightProbe = LineProbe(rightX.toDouble, yLow, yHigh),
        pattern = pattern
      )
    }
  }

  def generateProbes1B(pattern: PatternSpec, epsilon: Double): PatternProbes = {
    val (a, b) = pattern.position match {
      case BoundedRange(start, end) => (start, end)
      case _ => throw new IllegalArgumentException("1B requires BoundedRange position")
    }
    val yLow = pattern.yMin - epsilon
    val yHigh = pattern.yMax + epsilon

    val envelope = EnvelopeProbe(a.toDouble, yLow, b.toDouble, yHigh)
    PatternProbes(
      leftProbe = envelope,
      rightProbe = envelope,
      pattern = pattern
    )
  }


  def applyOverlapRewriting(patterns: List[PatternSpec]): List[PatternSpec] = {
    if (patterns.length <= 1) return patterns

    val result = patterns.toBuffer
    for (i <- 0 until result.length - 1) {
      val p1 = result(i)
      val p2 = result(i + 1)

      (p1.position, p2.position) match {
        case (FixedStart(s1), FixedStart(s2)) =>
          val p1MaxEnd = s1 + p1.maxLength - 1
          val p1MinEnd = s1 + p1.minLength - 1
          val p2Start = s2

          if (p1MaxEnd >= p2Start && p1.isDynamicLength) {
            val cappedMaxLen = p2Start - s1
            if (cappedMaxLen >= p1.minLength) {
              result(i) = p1.copy(maxLength = cappedMaxLen)
            }
          }
        case _ =>
      }
    }
    result.toList
  }


  def rewriteWholeSequenceProbes(
    windowQueries: List[((Double, Double, Double, Double), (Double, Double, Double, Double), String, Option[Any])],
    epsilon: Double,
    tsLength: Int = 128
  ): List[(ProbeGeometry, ProbeGeometry, Option[Any])] = {
    windowQueries.map { case (ls, rs, flag, constraints) =>
      val isDynamic = rs == (0.0, 0.0, 0.0, 0.0)

      if (isDynamic) {
        val leftProbe = LineProbe(ls._1, ls._2, ls._4)

        val rightX = constraints match {
          case Some(c) =>
            try {
              val lr = c.getClass.getMethod("lengthRange").invoke(c)
              lr match {
                case Some(range: (Int, Int) @unchecked) => ls._1 + range._2 - 1
                case _ => ls._1 + tsLength - 1
              }
            } catch {
              case _: Exception => ls._1 + tsLength - 1
            }
          case None => ls._1 + tsLength - 1
        }

        val rightProbe = LineProbe(rightX, ls._2, ls._4)
        (leftProbe.asInstanceOf[ProbeGeometry], rightProbe.asInstanceOf[ProbeGeometry], constraints)
      } else {
        val leftProbe = LineProbe(ls._1, ls._2, ls._4)
        val rightProbe = LineProbe(rs._1, rs._2, rs._4)
        (leftProbe.asInstanceOf[ProbeGeometry], rightProbe.asInstanceOf[ProbeGeometry], constraints)
      }
    }
  }


  private def parseValueRangePair(rangeStr: String): (Double, Double) = {
    val regex = """[\[\(]\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*[\]\)]""".r
    rangeStr.trim match {
      case regex(a, b) => (a.toDouble, b.toDouble)
      case _ => throw new IllegalArgumentException(s"Cannot parse value range: $rangeStr")
    }
  }

  private def parseDirection(dirStr: String): String = {
    dirStr.trim match {
      case "<+>" => "INCREASING"
      case "<->" => "DECREASING"
      case "<=>" => "FLAT"
      case "<~>" | "<*>" => "ANY"
      case _ => throw new IllegalArgumentException(s"Unknown direction: $dirStr")
    }
  }

  private def parseStepSize(stepStr: String): Double = {
    val s = stepStr.trim.replaceAll("\\*", "")
    if (s.isEmpty) 0.0
    else {
      try { s.toDouble }
      catch { case _: NumberFormatException => 0.0 }
    }
  }

  private def parseLength(lengthStr: String): (Int, Int) = {
    val fixedRegex = """\{\s*(\d+)\s*\}""".r
    val dynamicRegex = """\{\s*(\d+)\s*,\s*(\d+)\s*\}""".r
    lengthStr.trim match {
      case dynamicRegex(min, max) => (min.toInt, max.toInt)
      case fixedRegex(n) => (n.toInt, n.toInt)
      case _ => throw new IllegalArgumentException(s"Cannot parse length: $lengthStr")
    }
  }

  private def parseThreshold(queryString: String): Double = {
    val regex = """Threshold:\s*(\d+\.?\d*)""".r
    regex.findFirstMatchIn(queryString) match {
      case Some(m) => m.group(1).toDouble
      case None => 0.0
    }
  }

  private def smartSplit(s: String, delimiter: Char): List[String] = {
    val result = scala.collection.mutable.ListBuffer[String]()
    val current = new StringBuilder
    var depth = 0

    for (c <- s) {
      c match {
        case '[' | '(' | '{' => depth += 1; current += c
        case ']' | ')' | '}' => depth -= 1; current += c
        case `delimiter` if depth == 0 =>
          result += current.toString.trim
          current.clear()
        case _ => current += c
      }
    }
    if (current.nonEmpty) result += current.toString.trim
    result.toList
  }


  def probeToSql(probe: ProbeGeometry): String = probe match {
    case LineProbe(x, yMin, yMax) =>
      s"ST_SetSRID(ST_MakeLine(ST_Point($x, $yMin), ST_Point($x, $yMax)), 4326)"
    case EnvelopeProbe(xMin, yMin, xMax, yMax) =>
      s"ST_SetSRID(ST_MakeEnvelope($xMin, $yMin, $xMax, $yMax), 4326)"
  }

  def slopeConstraintSql(direction: String): String = direction match {
    case "INCREASING" => " AND segment_slope > 0"
    case "DECREASING" => " AND segment_slope < 0"
    case "FLAT" => " AND segment_slope = 0"
    case "ANY" | _ => ""
  }

  def fluctuationConstraintSql(direction: String): String = {
    if (!Config.USE_FLUCTUATION_FILTER) return ""
    direction match {
      case "INCREASING" | "DECREASING" | "FLAT" => " AND is_fluctuating = false"
      case _ => ""
    }
  }

  def valueRangeConstraintSql(yMin: Double, yMax: Double, epsilon: Double): String = {
    val low = yMin - epsilon
    val high = yMax + epsilon
    s" AND ST_YMax(time_series_segments) >= $low AND ST_YMin(time_series_segments) <= $high"
  }

  def generateSpatialIntersectSql(tableName: String, probes: PatternProbes): String = {
    val leftSql = probeToSql(probes.leftProbe)
    val rightSql = probeToSql(probes.rightProbe)
    val p = probes.pattern
    val slope = slopeConstraintSql(p.direction)
    val fluct = fluctuationConstraintSql(p.direction)
    val yRange = valueRangeConstraintSql(p.yMin, p.yMax, 0.0)

    if (probes.leftProbe == probes.rightProbe) {
      s"""SELECT DISTINCT time_series_id FROM $tableName
         |WHERE ST_Intersects(time_series_segments, $leftSql)$slope$fluct$yRange""".stripMargin
    } else {
      s"""SELECT DISTINCT time_series_id FROM $tableName
         |WHERE ST_Intersects(time_series_segments, $leftSql)$slope$fluct$yRange
         |INTERSECT
         |SELECT DISTINCT time_series_id FROM $tableName
         |WHERE ST_Intersects(time_series_segments, $rightSql)$slope$fluct$yRange""".stripMargin
    }
  }

  def generateBtreeSql(tableName: String, pattern: PatternSpec, epsilon: Double): String = {
    val slope = slopeConstraintSql(pattern.direction)
    val valueRange = valueRangeConstraintSql(pattern.yMin, pattern.yMax, epsilon)
    val fluct = fluctuationConstraintSql(pattern.direction)

    s"""SELECT DISTINCT time_series_id FROM $tableName
       |WHERE 1=1$slope$valueRange$fluct""".stripMargin
  }

  def generateMultiPatternBtreeIntersectSql(tableName: String, patterns: List[PatternSpec], epsilon: Double): String = {
    patterns.map(p => generateBtreeSql(tableName, p, epsilon)).mkString("\nINTERSECT\n")
  }

  def predicateForPattern(pattern: PatternSpec, epsilon: Double): String = {
    val slopeExpr = pattern.direction match {
      case "INCREASING" => Some("segment_slope > 0")
      case "DECREASING" => Some("segment_slope < 0")
      case "FLAT"       => Some("segment_slope = 0")
      case _            => None
    }
    val low = pattern.yMin - epsilon
    val high = pattern.yMax + epsilon
    val rangeExpr = Some(
      s"ST_YMax(time_series_segments) >= $low AND ST_YMin(time_series_segments) <= $high"
    )
    val fluctExpr =
      if (Config.USE_FLUCTUATION_FILTER &&
          (pattern.direction == "INCREASING" || pattern.direction == "DECREASING" || pattern.direction == "FLAT"))
        Some("is_fluctuating = false")
      else None

    val parts = List(slopeExpr, rangeExpr, fluctExpr).flatten
    if (parts.isEmpty) "TRUE" else parts.mkString(" AND ")
  }

  def generateMultiPatternCompositeSql(tableName: String, patterns: List[PatternSpec], epsilon: Double): String = {
    val preds = patterns.map(p => predicateForPattern(p, epsilon))
    val whereClause = preds.map(p => s"($p)").mkString(" OR ")
    val havingClause = preds.map(p => s"bool_or($p)").mkString(" AND ")
    s"""SELECT time_series_id
       |FROM $tableName
       |WHERE $whereClause
       |GROUP BY time_series_id
       |HAVING $havingClause""".stripMargin
  }

  def generateMultiPatternSpatialIntersectSql(tableName: String, probesList: List[PatternProbes]): String = {
    probesList.map(probes => generateSpatialIntersectSql(tableName, probes)).mkString("\nINTERSECT\n")
  }


  def estimateSelectivity(pattern: PatternSpec): Double = {
    val valueRangeWidth = math.abs(pattern.yMax - pattern.yMin)

    val dirFactor = pattern.direction match {
      case "INCREASING" | "DECREASING" => 0.5
      case "FLAT" => 0.7
      case _ => 1.0
    }

    val rangeFactor = math.min(valueRangeWidth / 5.0, 1.0)

    dirFactor * rangeFactor
  }

  def rankPatternsBySelectivity(patterns: List[PatternSpec]): List[Int] = {
    patterns.zipWithIndex
      .sortBy { case (p, _) => estimateSelectivity(p) }
      .map(_._2)
  }

  def selectMostSelectivePattern(patterns: List[PatternSpec]): Int = {
    rankPatternsBySelectivity(patterns).head
  }

  def generateBestOnlySpatialSql(tableName: String, probesList: List[PatternProbes], bestIdx: Int): String = {
    generateSpatialIntersectSql(tableName, probesList(bestIdx))
  }

  def generateBestOnlyBtreeSql(tableName: String, patterns: List[PatternSpec], bestIdx: Int, epsilon: Double): String = {
    generateBtreeSql(tableName, patterns(bestIdx), epsilon)
  }


  def generateCascadeFirstSpatialSql(tableName: String, probes: PatternProbes): String = {
    generateSpatialIntersectSql(tableName, probes)
  }

  def generateCascadeFirstBtreeSql(tableName: String, pattern: PatternSpec, epsilon: Double): String = {
    generateBtreeSql(tableName, pattern, epsilon)
  }

  def generateCascadeNextSpatialSql(tableName: String, probes: PatternProbes, candidateIdsBatch: Array[Long]): String = {
    val baseSql = generateSpatialIntersectSql(tableName, probes)
    addCandidateFilter(baseSql, candidateIdsBatch)
  }

  def generateCascadeNextBtreeSql(tableName: String, pattern: PatternSpec, epsilon: Double, candidateIdsBatch: Array[Long]): String = {
    val baseSql = generateBtreeSql(tableName, pattern, epsilon)
    addCandidateFilter(baseSql, candidateIdsBatch)
  }

  private def addCandidateFilter(sql: String, candidateIds: Array[Long]): String = {
    val idList = candidateIds.mkString(",")
    s"""SELECT time_series_id FROM ($sql) AS base_query
       |WHERE time_series_id = ANY(ARRAY[$idList]::bigint[])""".stripMargin
  }
}
