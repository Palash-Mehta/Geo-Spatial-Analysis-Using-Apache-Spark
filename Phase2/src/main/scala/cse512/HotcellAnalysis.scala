package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    pickupInfo.createOrReplaceTempView("nyctaxitrips2")
    pickupInfo = spark.sql("select x, y, z from nyctaxitrips2 where x >= " + minX + " and x <= " + maxX + " and y >= " + minY + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ)
    pickupInfo.createOrReplaceTempView("nyctaxitrips2")

    val dfForXi = spark.sql("SELECT x, y, z, COUNT(*) as xi FROM nyctaxitrips2 GROUP BY x, y, z");
    dfForXi.createOrReplaceTempView("_nyctt2")
    dfForXi.show()

    val dfForXNeighbors = spark.sql("SELECT t1.x, t1.y, t1.z, SUM(t2.xi) as sumNX, COUNT(t2.xi) as nNeighbors FROM _nyctt2 t1, _nyctt2 t2 WHERE (" +
      "(ABS(t1.x-t2.x) = 1 OR ABS(t1.x-t2.x) = 0) AND (ABS(t1.y-t2.y) = 1 OR ABS(t1.y-t2.y) = 0) AND (ABS(t1.z-t2.z) = 1 OR ABS(t1.z-t2.z) = 0)) GROUP BY t1.x, t1.y, t1.z")
    dfForXNeighbors.show()
    dfForXNeighbors.createOrReplaceTempView("_nyctt3")

    val sumX = spark.sql("SELECT SUM(_nyctt2.xi) as sumX FROM _nyctt2").first().getLong(0).toDouble
    val sumX2 = spark.sql("SELECT SUM(_nyctt2.xi * _nyctt2.xi) as sumX2 FROM _nyctt2").first().getLong(0).toDouble

    val meanX = (sumX / numCells.toDouble).toDouble

    val standardDeviationX = math.sqrt(sumX2.toDouble / numCells.toDouble - meanX.toDouble * meanX.toDouble)

    spark.udf.register("zScore", (cx: Int, cy: Int, cz: Int, sumNX: Double, nNeighbors: Int) => ((
      HotcellUtils.zScore(cx, cy, cz, minX, maxX, minY, maxY, minZ, maxZ, numCells.toDouble, sumNX.toDouble, nNeighbors, meanX.toDouble, standardDeviationX.toDouble)
      )))

    val finalDF = spark.sql("SELECT _.x, _.y, _.z FROM (SELECT x, y, z, zScore(_nyctt3.x, _nyctt3.y, _nyctt3.z, _nyctt3.sumNX, _nyctt3.nNeighbors) as zs FROM _nyctt3 ORDER BY zs DESC) _")
    finalDF.show()

    return finalDF
  }
}
