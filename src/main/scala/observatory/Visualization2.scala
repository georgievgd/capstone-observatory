package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Interaction.{Tile256Heatmapper, tileLocation}
import org.apache.log4j.Logger

import scala.collection.immutable.ListMap
import scala.collection.parallel.immutable.ParSeq
import scala.util.Random

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  val logger = Logger.getLogger(this.getClass)

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    val result =
      d00*(1-point.x)*(1-point.y) +
      d10*point.x*(1-point.y) +
      d01*(1-point.x)*point.y +
      d11*point.x*point.y
    result
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {
    val mapper = new Tile256GridHeatmapper(colors, tile)
    mapper.buildGridImage(grid, tile.toString)
  }

  def generateTemperaturesImage(year: Year, t: Tile, grid: GridLocation => Temperature): Unit = {
    generateImage(year, t, grid, "temperatures", Utils.temperatureColors)
  }

  def generateDeviationsImage(year: Year, t: Tile, grid: GridLocation => Temperature): Unit = {
    generateImage(year, t, grid, "deviations", Utils.deviationColors)
  }

  def generateImage(year: Year, t: Tile, grid: GridLocation => Temperature, folderName: String, colors: Iterable[(Temperature, Color)]): Unit = {
    val outputFile = new java.io.File("target/" + folderName + "/" + year + "/" + t.zoom + "/" + t.x + "-" + t.y + ".png")
    logger.debug("Output file path - " + outputFile.getAbsolutePath)
    import java.nio.file.Files
    val parentDir = outputFile.toPath.getParent
    if (!Files.exists(parentDir)) Files.createDirectories(parentDir)
    val img = visualizeGrid(grid, colors, t).output(outputFile)
  }

  class Tile256GridHeatmapper(newColors: Iterable[(Double, Color)], tile: Tile) extends Tile256Heatmapper(newColors, tile) {

    def buildGridImage(tempGrid: GridLocation => Temperature, imageID:String = Random.alphanumeric.take(10).mkString): Image = {
      //logger.debug("Image started - " + imageID)
      val lineTasks = for(y <- 0 until height) yield y
      val result: ParSeq[(Int, Array[Pixel])] = lineTasks.par.map(y => getGridImageLine(y, width, tempGrid, imageID))
      val pixels = ListMap(result.toList.sortBy(_._1):_*).foldLeft(new Array[Pixel](0))((res, t) => res ++: t._2)
      //logger.debug("Image completed - " + imageID)
      Image(width, height, pixels)
    }

    def getGridImageLine(row: Int, width: Int, tempGrid: GridLocation => Temperature, imageID: String): (Int, Array[Pixel]) = {
      val line = new Array[Pixel](width)
      for (x <- 0 until width) {
        val location = xyToLocation(x, row)
        val temp = estimatedLocationTemperature(tempGrid, location)//Visualization.predictTemperature(temperatures, xyToLocation(x, row))
        //logger.debug("Temp " + temp + " for x,y " + x + "," + row + " - " + xyToLocation(x, row))
        val color = Visualization.interpolateColor(colors, temp)
        line(x) = Pixel.apply(color.red, color.green, color.blue, alpha)
      }
      //logger.debug("Image [" + imageID + "] completed row " + row)
      (row, line)
    }

    def estimatedLocationTemperature(tempGrid: GridLocation => Temperature, location: Location): Temperature = {
      def rolloverHeight(value: Int): Int = {if (value>=90) 0 else value}
      def rolloverWidth(value: Int): Int = {if (value>=180) 0 else value}

      val point = CellPoint(location.lat-location.lat.floor, location.lon-location.lon.floor)
      val d00 = tempGrid(GridLocation(location.lat.floor.toInt, location.lon.floor.toInt))
      val d01 = tempGrid(GridLocation(location.lat.floor.toInt, rolloverWidth(location.lon.ceil.toInt)))
      val d10 = tempGrid(GridLocation(rolloverHeight(location.lat.ceil.toInt), location.lon.floor.toInt))
      val d11 = tempGrid(GridLocation(rolloverHeight(location.lat.ceil.toInt), rolloverWidth(location.lon.ceil.toInt)))
      bilinearInterpolation(point, d00, d01, d10, d11)
    }

  }

}
