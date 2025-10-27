package org.example.bigdata

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object QueryParser {

  val DATASET = Config.DATASET
  val params = Config.getParams()

  val epsilon = params.epsilon
  val defaultLower = params.yMin
  val defaultUpper = params.yMax

  case class SubpatternConstraints(
    valueRange: (Double, Double),
    length: Int,                              // For fixed-length patterns
    lengthRange: Option[(Int, Int)] = None,   // For dynamic-length patterns: Some((min, max))
    direction: Option[String] = None          // "INCREASING", "DECREASING", "FLAT", "ANY"
  )

  def parseRange(rangeStr: String): (Double, Double) = {
    val trimmed = rangeStr.trim
    if (trimmed.isEmpty) return (defaultLower, defaultUpper)
    val leftBracket = trimmed.head
    val rightBracket = trimmed.last
    val inner = trimmed.substring(1, trimmed.length - 1).trim
    val parts = inner.split(",").map(_.trim)
    val lowerStr = if (parts.length > 0) parts(0) else ""
    val upperStr = if (parts.length > 1) parts(1) else ""

    val lowerParsed = if (lowerStr.isEmpty) defaultLower else lowerStr.toDouble
    val upperParsed = if (upperStr.isEmpty) defaultUpper else upperStr.toDouble

    val lower = if (leftBracket == '[') lowerParsed - epsilon else lowerParsed
    val upper = if (rightBracket == ']') upperParsed + epsilon else upperParsed

    (lower, upper)
  }

  def parseDirection(subpattern: String): Option[String] = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim

    if (cleaned.contains("<+>")) Some("INCREASING")
    else if (cleaned.contains("<->")) Some("DECREASING")
    else if (cleaned.contains("<=>")) Some("FLAT")
    else if (cleaned.contains("<~>")) Some("ANY")
    else None  // No direction specified, treat as ANY
  }

  def parseLength(subpattern: String): (Int, Option[(Int, Int)], Boolean) = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim

    val dynamicLengthRegex = new Regex("\\{(\\d+)\\s*,\\s*(\\d+)\\}")
    dynamicLengthRegex.findFirstMatchIn(cleaned) match {
      case Some(m) =>
        val min = m.group(1).toInt
        val max = m.group(2).toInt
        (min, Some((min, max)), true)  // Dynamic length
      case None =>
        val fixedLengthRegex = new Regex("\\{(\\d+)\\}")
        val length = fixedLengthRegex.findFirstMatchIn(cleaned).map(_.group(1).toInt).getOrElse(0)
        (length, None, false)  // Fixed length
    }
  }

  def parseSubpattern(subpattern: String): SubpatternConstraints = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim

    val (length, lengthRange, isDynamic) = parseLength(subpattern)

    val rangeRegex = new Regex("\\[[^\\]]+\\]|\\([^\\)]+\\)")
    val valueRange = rangeRegex.findFirstIn(cleaned).map(parseRange).getOrElse((defaultLower, defaultUpper))

    val direction = parseDirection(subpattern)

    SubpatternConstraints(valueRange, length, lengthRange, direction)
  }

  def parsePattern(pattern: String): List[((Double, Double, Double, Double),
    (Double, Double, Double, Double), String, Option[SubpatternConstraints])] = {
    val trimmedPattern = pattern.trim.stripPrefix("{").stripSuffix("}")
    val subPatternRegex = new Regex("\\|[^|]+\\|")
    val subPatterns = subPatternRegex.findAllIn(trimmedPattern).toList
    val modifiedPattern = subPatternRegex.replaceAllIn(trimmedPattern, "SUBPATTERN")
    val elements = modifiedPattern.split(",(?![^\\[\\]]*\\])").map(_.trim).toList

    var currentIndex = 0
    var subPatternIndex = 0
    var stopParsing = false
    val windows = ListBuffer[((Double, Double, Double, Double), (Double, Double, Double, Double), String, Option[SubpatternConstraints])]()

    val elementsIterator = elements.iterator
    while (elementsIterator.hasNext && !stopParsing) {
      val element = elementsIterator.next()

      element match {
        case e if e == "SUBPATTERN" =>
          val originalSubpattern = subPatterns(subPatternIndex).trim
          val constraints = parseSubpattern(originalSubpattern)

          
          if (constraints.lengthRange.isDefined) {
            
            val leftSegment = generateDynamicSubpatternLeftWindow(currentIndex, originalSubpattern)
            val dummyRightSegment = (0.0, 0.0, 0.0, 0.0)
            windows += ((leftSegment, dummyRightSegment, originalSubpattern, Some(constraints)))
            stopParsing = true
            
          } else {
            val (leftSegment, rightSegment) = generateSubpatternWindow(currentIndex, originalSubpattern, constraints.length)
            windows += ((leftSegment, rightSegment, originalSubpattern, Some(constraints)))
            currentIndex += constraints.length
          }
          subPatternIndex += 1

        case e if e.startsWith("[") || e.startsWith("(") =>
          val window = generateValueRangeWindow(currentIndex, e)
          windows += ((window, window, "nonsubpattern", None))
          currentIndex += 1

        case singleton if singleton.forall(c => c.isDigit || c == '.' || c == '-') =>
          val value = singleton.toDouble
          val window = (currentIndex.toDouble, value, currentIndex.toDouble, value)
          windows += ((window, window, "nonsubpattern", None))
          currentIndex += 1

        case _ => // Ignore other patterns
      }
    }
    windows.toList
  }

  def parsePatternLegacy(pattern: String): List[((Double, Double, Double, Double),
    (Double, Double, Double, Double), String)] = {
    parsePattern(pattern).map { case (leftWindow, rightWindow, flag, _) =>
      (leftWindow, rightWindow, flag)
    }
  }

  private def generateValueRangeWindow(index: Int, range: String): (Double, Double, Double, Double) = {
    val (lower, upper) = parseRange(range)
    (index.toDouble, lower, index.toDouble, upper)
  }

  private def generateSubpatternWindow(index: Int, subpattern: String, length: Int): ((Double, Double, Double, Double), (Double, Double, Double, Double)) = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim
    val rangeRegex = new Regex("\\[[^\\]]+\\]|\\([^\\)]+\\)")
    rangeRegex.findFirstIn(cleaned) match {
      case Some(fullRangeStr) =>
        val (lower, upper) = parseRange(fullRangeStr)
        val leftSegment = (index.toDouble, lower, index.toDouble, upper)
        val rightSegment = ((index.toDouble + length - 1), lower, (index.toDouble + length - 1), upper)
        (leftSegment, rightSegment)
      case None =>
        ((0.0, 0.0, 0.0, 0.0), (0.0, 0.0, 0.0, 0.0))
    }
  }

  private def generateDynamicSubpatternLeftWindow(index: Int, subpattern: String): (Double, Double, Double, Double) = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim
    val rangeRegex = new Regex("\\[[^\\]]+\\]|\\([^\\)]+\\)")
    rangeRegex.findFirstIn(cleaned) match {
      case Some(fullRangeStr) =>
        val (lower, upper) = parseRange(fullRangeStr)
        (index.toDouble, lower, index.toDouble, upper)
      case None =>
        (0.0, 0.0, 0.0, 0.0)
    }
  }
}
