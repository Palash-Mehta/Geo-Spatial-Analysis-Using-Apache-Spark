package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App {
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => {
      val TotalPoints = pointString.split(',').map(x => x.trim.toDouble)
      val TotalRectangle = queryRectangle.split(',').map(x => x.trim.toDouble)
      val point_x = TotalPoints(0)
      val point_y = TotalPoints(1)

      var rectangle_start_x = 0.0
      var rectangle_start_y = 0.0
      var rectangle_end_x = 0.0
      var rectangle_end_y = 0.0

      //Decide the x coordinates
      if (TotalRectangle(0) > TotalRectangle(2)) {
        rectangle_start_x = TotalRectangle(2)
        rectangle_end_x = TotalRectangle(0)
      }
      else if (TotalRectangle(0) < TotalRectangle(2)) {
        rectangle_start_x = TotalRectangle(0)
        rectangle_end_x = TotalRectangle(2)
      }

      if (TotalRectangle(1) > TotalRectangle(3)) {
        rectangle_start_y = TotalRectangle(3)
        rectangle_end_y = TotalRectangle(1)
      }
      else if (TotalRectangle(1) < TotalRectangle(3)) {
        rectangle_start_y = TotalRectangle(1)
        rectangle_end_y = TotalRectangle(3)
      }
      //Decide the Y coordinates
      if (point_x >= rectangle_start_x && point_x <= rectangle_end_x && point_y >= rectangle_start_y && point_y <= rectangle_end_y)
        true
      else
        false

    })

    val resultDf = spark.sql("select * from point where ST_Contains('" + arg2 + "',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => {
      val TotalPoints = pointString.split(',').map(x => x.trim.toDouble)
      val TotalRectangle = queryRectangle.split(',').map(x => x.trim.toDouble)
      val point_x = TotalPoints(0)
      val point_y = TotalPoints(1)

      var rectangle_start_x = 0.0
      var rectangle_start_y = 0.0
      var rectangle_end_x = 0.0
      var rectangle_end_y = 0.0

      //Decide the x coordinates
      if (TotalRectangle(0) > TotalRectangle(2)) {
        rectangle_start_x = TotalRectangle(2)
        rectangle_end_x = TotalRectangle(0)
      }
      else if (TotalRectangle(0) < TotalRectangle(2)) {
        rectangle_start_x = TotalRectangle(0)
        rectangle_end_x = TotalRectangle(2)
      }

      if (TotalRectangle(1) > TotalRectangle(3)) {
        rectangle_start_y = TotalRectangle(3)
        rectangle_end_y = TotalRectangle(1)
      }
      else if (TotalRectangle(1) < TotalRectangle(3)) {
        rectangle_start_y = TotalRectangle(1)
        rectangle_end_y = TotalRectangle(3)
      }
      //Decide the Y coordinates
      if (point_x >= rectangle_start_x && point_x <= rectangle_end_x && point_y >= rectangle_start_y && point_y <= rectangle_end_y)
        true
      else
        false

    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => {
      val x1 = pointString1.split(",")(0).toDouble
      val y1 = pointString1.split(",")(1).toDouble

      val x2 = pointString2.split(",")(0).toDouble
      val y2 = pointString2.split(",")(1).toDouble

      if (((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1)) <= distance * distance) {
        true
      }
      else {
        false
      }
    })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'" + arg2 + "'," + arg3 + ")")

    resultDf.show()
    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => {
      val x1 = pointString1.split(",")(0).toDouble
      val y1 = pointString1.split(",")(1).toDouble

      val x2 = pointString2.split(",")(0).toDouble
      val y2 = pointString2.split(",")(1).toDouble

      if (((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1)) <= distance * distance) {
        true
      }
      else {
        false
      }
    })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")")
    resultDf.show()

    return resultDf.count()
  }
}
