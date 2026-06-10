package org.example.bigdata

import org.example.bigdata.SubsequenceQueryParser._

object CostBasedQueryDecision {

  case class GridCell(grid_id: String, xMin: Double, yMin: Double, xMax: Double, yMax: Double)

  sealed trait QueryStrategy
  case object UseSpatial extends QueryStrategy
  case object UseBtree extends QueryStrategy


  def estimateSpatialCost(
    gridCellCounts: Map[String, Int],
    gridCells: List[GridCell],
    probe: ProbeGeometry
  ): Int = {
    val intersecting = gridCells.filter(cell => probeIntersectsCell(probe, cell))
    intersecting.map(c => gridCellCounts.getOrElse(c.grid_id, 0)).sum
  }

  def estimateSpatialIntersectCost(
    gridCellCounts: Map[String, Int],
    gridCells: List[GridCell],
    probes: PatternProbes
  ): Int = {
    if (probes.leftProbe == probes.rightProbe) {
      estimateSpatialCost(gridCellCounts, gridCells, probes.leftProbe)
    } else {
      val leftCost = estimateSpatialCost(gridCellCounts, gridCells, probes.leftProbe)
      val rightCost = estimateSpatialCost(gridCellCounts, gridCells, probes.rightProbe)
      math.min(leftCost, rightCost)
    }
  }

  private def probeIntersectsCell(probe: ProbeGeometry, cell: GridCell): Boolean = {
    probe match {
      case LineProbe(x, yMin, yMax) =>
        x >= cell.xMin && x <= cell.xMax &&
          yMax >= cell.yMin && yMin <= cell.yMax

      case EnvelopeProbe(xMin, yMin, xMax, yMax) =>
        xMax >= cell.xMin && xMin <= cell.xMax &&
          yMax >= cell.yMin && yMin <= cell.yMax
    }
  }


  def estimateBtreeCost(
    totalSegments: Int,
    direction: String,
    yMin: Double,
    yMax: Double,
    dataYMin: Double,
    dataYMax: Double
  ): Int = {
    val slopeSelectivity = direction match {
      case "INCREASING" | "DECREASING" => 0.5
      case "FLAT" => 0.05
      case "ANY" | _ => 1.0
    }

    val dataRange = dataYMax - dataYMin
    val yRangeSelectivity = if (dataRange > 0) {
      math.min(1.0, (yMax - yMin) / dataRange)
    } else 1.0

    val fluctSelectivity = direction match {
      case "INCREASING" | "DECREASING" | "FLAT" => 0.7
      case _ => 1.0
    }

    (totalSegments * slopeSelectivity * yRangeSelectivity * fluctSelectivity).toInt
  }


  def decide(
    gridCellCounts: Map[String, Int],
    gridCells: List[GridCell],
    probes: PatternProbes,
    totalSegments: Int,
    dataYMin: Double,
    dataYMax: Double
  ): QueryStrategy = {
    val spatialCost = estimateSpatialIntersectCost(gridCellCounts, gridCells, probes)
    val btreeCost = estimateBtreeCost(
      totalSegments,
      probes.pattern.direction,
      probes.pattern.yMin,
      probes.pattern.yMax,
      dataYMin,
      dataYMax
    )

    if (spatialCost <= btreeCost) UseSpatial else UseBtree
  }

  def selectBestPattern(
    gridCellCounts: Map[String, Int],
    gridCells: List[GridCell],
    probesList: List[PatternProbes]
  ): Int = {
    if (probesList.isEmpty) return 0

    val costs = probesList.map { probes =>
      estimateSpatialIntersectCost(gridCellCounts, gridCells, probes)
    }

    costs.zipWithIndex.minBy(_._1)._2
  }


  sealed trait MultiPatternStrategy
  case class UseBestPatternOnly(patternIndex: Int) extends MultiPatternStrategy
  case object UseIntersectAll extends MultiPatternStrategy

  def decideMultiPatternStrategy(
    gridCellCounts: Map[String, Int],
    gridCells: List[GridCell],
    probesList: List[PatternProbes]
  ): MultiPatternStrategy = {
    if (probesList.size <= 1) return UseBestPatternOnly(0)

    val costs = probesList.map { probes =>
      estimateSpatialIntersectCost(gridCellCounts, gridCells, probes)
    }

    val bestIdx = costs.zipWithIndex.minBy(_._1)._2
    val bestPatternCost = costs(bestIdx)

    val intersectAllCost = costs.min

    if (probesList.size >= 3 || intersectAllCost < bestPatternCost * 0.8) {
      UseIntersectAll
    } else {
      UseBestPatternOnly(bestIdx)
    }
  }
}
