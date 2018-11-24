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
    val weightedTemps = temperatures.map(t => (getWeight(t._1.lat, t._1.lon, location.lat, location.lon), t._2))

    val matchingLocation = weightedTemps.filter(_._1 == 1)
    if(matchingLocation.size > 0){
      //println(matchingLocation.head._2)
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
    /*val spark = Spark.session

    try{
      val points = for {x <- 90 to -89 by -1
                        y <- -180 to 179
      } yield (x*(-10000L)+y, (x, y))
      //logger.debug("Raw points " + points.length + ", with first 20:\n" + points.take(10).mkString("\n"))
      val pointsRdd: RDD[(Long, (Int, Int))] = spark.sparkContext.parallelize(points)
      val tempRawRdd: RDD[(Location, Temperature)] = spark.sparkContext.parallelize(temperatures.toSeq)
      //logger.debug("Tempreratures raw RDD count " + tempRawRdd.count() + ", with first 10:\n" + tempRawRdd.take(10).mkString("\n"))
      val tempRdd: RDD[(Long, (Location, Temperature))] = tempRawRdd.map(t => (math.round(t._1.lat)*(-10000) + math.round(t._1.lon), t))
      logger.debug("Tempreratures RDD count " + tempRdd.count() + ", with first 10:\n" + tempRdd.take(10).mkString("\n"))
      val joinedRdd: RDD[(Long, ((Int, Int), Option[(Location, Temperature)]))] = pointsRdd.leftOuterJoin(tempRdd)
      logger.debug("Joined RDD count " + joinedRdd.count() + ", with first 30:\n" + joinedRdd.take(30).mkString("\n"))
      val pixelPairRdd: RDD[(Long, Pixel)] = joinedRdd.mapValues(ttt =>
      {
        val temp = ttt._2 match
        {
          case Some(tuple) => tuple._2
          case None => Double.MaxValue
        }
        getPixel(temperatures, colors, ttt._1._1, ttt._1._2, temp)
      })
      logger.debug("pixelPairRdd " + pixelPairRdd.count() + ", with first 10:\n" + pixelPairRdd.take(10).mkString("\n"))
      val pixelMap = pixelPairRdd.reduceByKey((p1,p2) => p1).sortBy(_._1)
      logger.debug("PixelMap " + pixelMap.count() + ", with first 10:\n" + (for(p <- pixelMap.take(10)) yield p._1+"("+p._2.red+","+p._2.green+","+p._2.blue+")").mkString("\n"))
      val pixels = pixelMap.map(_._2).collect()
      logger.debug("Pixels " + pixels.length + ", with first 10:\n" + (for(p <- pixels.take(10)) yield "("+p.red+","+p.green+","+p.blue+")").mkString("\n"))
      val image = Image(360, 180, pixels)
      image
    }catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }*/
    val mapper = new GlobalHeatmapper(colors)
    mapper.buildImage(temperatures)
  }

  def getPixel(temperatures: Iterable[(Location, Temperature)]
               , points: Iterable[(Temperature, Color)]
               , x: Int
               , y: Int
               , currentTemp: Temperature
               , alpha: Int = 255): Pixel = {
    var temp: Double = 0.0
    if (currentTemp != Double.MaxValue) {
      temp = currentTemp
    } else {
      temp = predictTemperature(temperatures, Location(x, y))
    }
    val color = interpolateColor(points, temp)
    val pixel = Pixel(color.red, color.green, color.blue, alpha)
    pixel
  }




}
