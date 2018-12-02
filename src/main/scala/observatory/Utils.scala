package observatory

import java.time.LocalDate

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Utils {

  def f2c(fahr: Double): Double = {
    (fahr - 32) * 5/9
  }

  val EARTH_RADIUS  = 6371;
  val PI = 3.14159265359;

  def sphericalDistance(loc1: Location, loc2: Location): Double = {
    if(loc1.lat == (-1)*loc2.lat && (math.abs(loc1.lon) + math.abs(loc2.lon)) == 180){
      EARTH_RADIUS * PI
    } else if (loc1.lat == loc2.lat && loc1.lon == loc2.lon) {
      0.0
    } else{
      val cAngle = centralAngle1(loc1, loc2)
      EARTH_RADIUS * cAngle
    }
  }

  def centralAngle1(loc1: Location, loc2: Location): Double = {
    math.acos(math.sin(loc1.lat*PI/180)*math.sin(loc2.lat*PI/180) + math.cos(loc1.lat*PI/180)*math.cos(loc2.lat*PI/180)*math.cos(math.abs(loc1.lon*PI/180-loc2.lon*PI/180)))
  }

  def centralAngle2(loc1: Location, loc2: Location): Double = {
    val dx = math.cos(loc2.lat)*math.cos(loc2.lon) - math.cos(loc1.lat)*math.cos(loc1.lon)
    val dy = math.cos(loc2.lat)*math.sin(loc2.lon) - math.cos(loc1.lat)*math.sin(loc1.lon)
    val dz = math.sin(loc2.lat) - math.sin(loc1.lat)
    val c = math.sqrt(math.pow(dx,2) + math.pow(dy,2) + math.pow(dz,2))
    2*math.asin(c/2)
  }

  val temperatureColors: Seq[(Temperature,Color)] = Seq(
    (0, Color(0, 255, 255)),
    (60, Color(255, 255, 255)),
    (-15, Color(0, 0, 255)),
    (12, Color(255, 255, 0)),
    (32, Color(255, 0, 0)),
    (-27, Color(255, 0, 255)),
    (-60, Color(0, 0, 0)),
    (-50, Color(33, 0, 107))
  )

  val deviationColors: Seq[(Temperature,Color)] = Seq(
    (7, Color(0, 0, 0)),
    (4, Color(255, 0, 0)),
    (2, Color(255, 255, 0)),
    (0, Color(255, 255, 255)),
    (-2, Color(0, 255, 255)),
    (-7, Color(0, 0, 255))
  )

}
