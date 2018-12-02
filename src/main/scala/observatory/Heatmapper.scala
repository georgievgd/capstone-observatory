package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.log4j.Logger

import scala.collection.immutable.ListMap
import scala.collection.parallel.immutable.ParSeq
import scala.util.Random

trait Heatmapper {
  val logger = Logger.getLogger(this.getClass)

  val alpha: Int
  val width: Int
  val height: Int
  val colors: Iterable[(Temperature, Color)]

  def xyToLocation(x: Int, y: Int): Location

  def buildImage(temperatures: Iterable[(Location, Double)], imageID:String = Random.alphanumeric.take(10).mkString): Image = {
    logger.debug("Image started - " + imageID)
    val lineTasks = for(y <- 0 until height) yield y
    val result: ParSeq[(Int, Array[Pixel])] = lineTasks.par.map(y => getImageLine(y, width, temperatures, imageID))
    val pixels = ListMap(result.toList.sortBy(_._1):_*).foldLeft(new Array[Pixel](0))((res, t) => res ++: t._2)
    logger.debug("Image completed - " + imageID)
    Image(width, height, pixels)
  }

  def getImageLine(row: Int, width: Int, temperatures: Iterable[(Location, Double)], imageID: String): (Int, Array[Pixel]) = {
    val line = new Array[Pixel](width)
    for (x <- 0 until width) {
      val temp = Visualization.predictTemperature(temperatures, xyToLocation(x, row))
      //logger.debug("Temp " + temp + " for x,y " + x + "," + row + " - " + xyToLocation(x, row))
      val color = Visualization.interpolateColor(colors, temp)
      line(x) = Pixel.apply(color.red, color.green, color.blue, alpha)
    }
    logger.debug("Image [" + imageID + "] completed row " + row)
    (row, line)
  }
}

class GlobalHeatmapper(newColors: Iterable[(Temperature, Color)] = Utils.temperatureColors) extends Heatmapper {
  val alpha = 255
  val width = 360
  val height = 180
  override val colors: Iterable[(Temperature, Color)] = newColors

  def xyToLocation(x: Int, y: Int): Location = Location(90 - y, x - 180)

  def xyToGridLocation(x: Int, y: Int): GridLocation = {
    val location = xyToLocation(x, y)
    GridLocation(location.lat.toInt, location.lon.toInt)
  }

  def buildGridImage(tempGrid: GridLocation => Temperature, imageID:String = Random.alphanumeric.take(10).mkString): Image = {
    logger.debug("Image started - " + imageID)
    val lineTasks = for(y <- 0 until height) yield y
    val result: ParSeq[(Int, Array[Pixel])] = lineTasks.par.map(y => getGridImageLine(y, width, tempGrid, imageID))
    val pixels = ListMap(result.toList.sortBy(_._1):_*).foldLeft(new Array[Pixel](0))((res, t) => res ++: t._2)
    logger.debug("Image completed - " + imageID)
    Image(width, height, pixels)
  }

  def getGridImageLine(row: Int, width: Int, tempGrid: GridLocation => Temperature, imageID: String): (Int, Array[Pixel]) = {
    val line = new Array[Pixel](width)
    for (x <- 0 until width) {
//      val location = xyToLocation(x, row)
//      val point = CellPoint(x-x.floor, row-row.floor)
      val temp = tempGrid(xyToGridLocation(x, row))//Visualization.predictTemperature(temperatures, xyToLocation(x, row))
      //logger.debug("Temp " + temp + " for x,y " + x + "," + row + " - " + xyToLocation(x, row))
      val color = Visualization.interpolateColor(colors, temp)
      line(x) = Pixel.apply(color.red, color.green, color.blue, alpha)
    }
    logger.debug("Image [" + imageID + "] completed row " + row)
    (row, line)
  }

}
