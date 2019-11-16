package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
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

  }

  // YOU NEED TO CHANGE THIS PART

}
