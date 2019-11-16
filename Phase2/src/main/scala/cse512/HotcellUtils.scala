package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.math.sqrt

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def zScore(cx: Int, cy: Int, cz: Int, minCX: Double, maxCX: Double, minCY: Double, maxCY: Double, minCZ: Double, maxCZ: Double, n: Double, sumNX: Double, nNeighbors: Int, meanX: Double, standardDeviationX: Double): Double = {
    var score = 0.0

    var nN = 0
    nN = nNeighbors


    val numerator = sumNX - nN * meanX
    val denominator = standardDeviationX * sqrt((nN * n - nN * nN).toDouble / (n - 1.0).toDouble).toDouble

    if (denominator != 0) {
      score = numerator.toDouble / denominator
    }

    return score
  }

}
