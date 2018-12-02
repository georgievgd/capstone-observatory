package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
;

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  val logger = Logger.getLogger(this.getClass)
  //logger.setLevel(Level.DEBUG)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    //logger.debug("Predict temp for " + location)
    val weightedTemps = temperatures.map(t => (getWeight(t._1.lat, t._1.lon, location.lat, location.lon), t._2))
    val matchingLocation = weightedTemps.filter(_._1 == 1)
    if(matchingLocation.size > 0){
      matchingLocation.head._2
    }else {
      val numeratorAgg = weightedTemps.map(t => t._1 * t._2).reduce(_ + _)
      val denominatorAgg = weightedTemps.map(t => t._1).reduce(_ + _)
      val result = numeratorAgg / denominatorAgg
      //println(result)
      result
    }
  }

  val POWER = 2

  def getWeight(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double  = {
    val dist = Utils.sphericalDistance(Location(lat1, lon1), Location(lat2, lon2))
//    println(s"Distance for points $lat1,$lon1 and $lat2, $lon2 is $dist")
    if(dist == 0.0) 1
    else 1 / math.pow(dist, POWER)
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val tempBounds = temperatureBounds(points, value)
    if(tempBounds._1 == null)
      lowestTempColor(points)
    else if (tempBounds._2 == null)
      highestTempColor(points)
    else {
      val r = intepolateColorValue(value, tempBounds._1._1, tempBounds._2._1, tempBounds._1._2.red, tempBounds._2._2.red)
      val g = intepolateColorValue(value, tempBounds._1._1, tempBounds._2._1, tempBounds._1._2.green, tempBounds._2._2.green)
      val b = intepolateColorValue(value, tempBounds._1._1, tempBounds._2._1, tempBounds._1._2.blue, tempBounds._2._2.blue)
      Color(r,g,b)
    }
  }

  def temperatureBounds (scale: Iterable[(Temperature, Color)], value: Temperature): ((Temperature, Color), (Temperature, Color)) = {
    val diffs = scale.map(pair => (pair, value - pair._1))
    val lowerBoundFiltered = diffs.filter(_._2 >= 0)
    val lowerBound = if(lowerBoundFiltered.isEmpty) null else lowerBoundFiltered.reduce((pp1,pp2) => if(pp2._2<pp1._2) pp2 else pp1)
    val upperBoundFilered = diffs.filter(_._2 < 0)
    val upperBound = if(upperBoundFilered.isEmpty) null else upperBoundFilered.reduce((pp1,pp2) => if(pp2._2>pp1._2) pp2 else pp1)
    (if(lowerBound != null) lowerBound._1 else null,
      if(upperBound != null) upperBound._1 else null)
  }

  def lowestTempColor(scale: Iterable[(Temperature, Color)]): Color = {
    scale.reduce((x, y) => if(x._1 <= y._1) x else y)._2
  }

  def highestTempColor(scale: Iterable[(Temperature, Color)]): Color = {
    scale.reduce((x, y) => if(x._1 >= y._1) x else y)._2
  }

  def intepolateColorValue(x: Double, x0: Double, x1: Double, y0: Int, y1: Int): Int = {
    math.round((y0*(x1-x) + y1*(x-x0)) / (x1-x0)).toInt
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val mapper = new GlobalHeatmapper(colors)
    mapper.buildImage(temperatures)
  }

}
