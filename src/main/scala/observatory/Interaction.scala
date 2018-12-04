package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.log4j.Logger

import scala.math._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {
  val logger = Logger.getLogger(this.getClass)

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    tile2location(tile)
  }

  def tile2location(tile: Tile): Location = {
    val lat = toDegrees(atan(sinh(Utils.PI*(1.0 - 2.0*tile.y.toDouble/(1<<tile.zoom)))))
    val lon = tile.x.toDouble/(1<<tile.zoom)*360.0 - 180.0
    //logger.debug(s"Tile $tile converted to Location($lat, $lon)")
    Location(lat,lon)
  }

  def location2tile(l: Location, zoom: Int): Tile = {
    val x = ((l.lon+180.0)/360.0*(1<<zoom)).toInt
    val y = ((1-log(tan(toRadians(l.lat)) + 1/cos(toRadians(l.lat))) / Utils.PI) / 2.0*(1<<zoom)).toInt
    Tile(x, y, zoom)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val mapper = new Tile256Heatmapper(colors, tile)
    mapper.buildImage(temperatures, tile.toString)
  }

  def generateImage(year: Year, t: Tile, temperatures: Iterable[(Location, Temperature)]): Unit = {
    val outputFile = new java.io.File("target/temperatures/" + year + "/" + t.zoom + "/" + t.x + "-" + t.y + ".png")
    logger.debug("Output file path - " + outputFile.getAbsolutePath)
    import java.nio.file.Files
    val parentDir = outputFile.toPath.getParent
    if (!Files.exists(parentDir)) Files.createDirectories(parentDir)
    val img = tile(temperatures, Utils.temperatureColors, t).output(outputFile)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    val tiles: Seq[Tile] = for {z <- 0 to 3
                     y <- 0 until math.pow(2, z).toInt
                     x <- 0 until math.pow(2, z).toInt
                    } yield Tile(x,y,z)
    //logger.debug("Tiles" + tiles.mkString("\n"))
    for(tuple <- yearlyData){
      logger.debug("************ Generate tiles for year - " + tuple._1)
      tiles.map(t => {
        //logger.debug("******************* Start " + t)
        generateImage(tuple._1, t, tuple._2)
        //logger.debug("*********** Finish " + t)
        //Thread.sleep(10000)
      })

    }
  }

  class Tile256Heatmapper(newColors: Iterable[(Double, Color)], tile: Tile) extends Heatmapper {
    val alpha = 127
    val width = 256
    val height = 256
    val colors = newColors

    // Tile offset of this tile in the zoom+8 coordinate system
    val x0 = math.pow(2.0,  8).toInt * tile.x
    val y0 = math.pow(2.0, 8).toInt * tile.y

    def xyToLocation(x: Int, y: Int): Location = {
      tileLocation(Tile(x+x0, y+y0, tile.zoom + 8))
    }
  }

}
