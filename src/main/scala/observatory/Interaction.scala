package observatory

import java.nio.file.Path

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.getPixel
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

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
    /*val spark = Spark.session

    val points = for {lat <- 0 to 255
                      lon <- 0 to 255
    } yield (lat, lon)

    val subtiles: RDD[(Int, Tile)] = spark.sparkContext.parallelize(points)
      .map(p => {
        ((p._1*1000+p._2),
          Tile(
            p._1+tile.x*(pow(2,8).toInt),
            p._2+tile.y*(pow(2,8).toInt),
            tile.zoom+8))
      })
    logger.debug("Subtiles " + subtiles.count() + ", with first 5:\n" + subtiles.take(5).mkString("\n"))
    val tempTiles: RDD[(Int, ((Location, Temperature), (Tile, Boolean)))] = spark.sparkContext.parallelize(temperatures.toSeq)
      .map(tuple => {
        val absoluteTile = location2tile(tuple._1, tile.zoom+8)
        val rX = absoluteTile.x - tile.x*(pow(2,8).toInt)
        val rY = absoluteTile.y - tile.y*(pow(2,8).toInt)
        val isInCurrentTile = rX > 0 && rX < 256 && rY > 0 && rY < 256
        (rX*1000+rY, (tuple, (absoluteTile, isInCurrentTile)))
      })
    logger.debug("Temperature tiles " + tempTiles.count())
    val validTempTiles: RDD[(Year, ((Location, Temperature), (Tile, Boolean)))] = tempTiles.filter(_._2._2._2)
    logger.debug("FILTERED>>> Temperature tiles " + validTempTiles.count() + ", with first 5:\n" + validTempTiles.take(5).mkString("\n"))
    val joinedRdd: RDD[(Int, (Tile, Option[((Location, Temperature), (Tile, Boolean))]))] = subtiles.leftOuterJoin(validTempTiles)
    //logger.debug("JoinedRDD " + joinedRdd.count() + ", with first 10:\n" + joinedRdd.take(10).mkString("\n"))
    val pixelPairRdd = joinedRdd.mapValues(v => {
      val temp: Temperature = v._2 match
      {
        case Some(tuple) => tuple._1._2
        case None => Double.MaxValue
      }
      getPixel(temperatures, colors, v._1.x, v._1.y, temp, 127)
    })

    //logger.debug("pixelPairRdd " + pixelPairRdd.count() + ", with first 10:\n" + pixelPairRdd.take(10).mkString("\n"))
    val pixelMap: RDD[(Int, Pixel)] = pixelPairRdd.reduceByKey((p1, p2) => p1).sortBy(_._1)
    //logger.debug("PixelMap " + pixelMap.count() + ", with first 5:\n" + (for(p <- pixelMap.take(5)) yield p._1+"("+p._2.red+","+p._2.green+","+p._2.blue+")").mkString("\n"))
    val pixels: Array[Pixel] = pixelMap.map(_._2).collect()
    logger.debug("Pixels " + pixels.length + ", with first 5:\n" + (for(p <- pixels.take(5)) yield "("+p.red+","+p.green+","+p.blue+","+p.alpha+")").mkString("\n"))
    val image: Image = Image(256, 256, pixels)
    image*/
    val mapper = new Tile256Heatmapper(colors, tile)
    mapper.buildImage(temperatures, tile.toString)
  }

  def generateImage(year: Year, t: Tile, temperatures: Iterable[(Location, Temperature)]): Unit = {
    val outputFile = new java.io.File("target/temperatures/" + year + "/" + t.zoom + "/" + t.x + "-" + t.y + ".png")
    logger.debug("Output file path - " + outputFile.getAbsolutePath)
    import java.nio.file.Files
    val parentDir = outputFile.toPath.getParent
    if (!Files.exists(parentDir)) Files.createDirectories(parentDir)
    val img = tile(temperatures, Utils.colorScale, t)
    //Image(1, 1, Array(Pixel(0,0,0,0)))
    img.output(outputFile)
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
    val tiles: Seq[Tile] = for {z <- 1 to 3
                     y <- 0 until math.pow(2, z).toInt
                     x <- 0 until math.pow(2, z).toInt
                    } yield Tile(x,y,z)
    //logger.debug("Tiles" + tiles.mkString("\n"))
    for(tuple <- yearlyData){
      logger.debug("Generate tiles for year - " + tuple._1)
      tiles.map(t => {
        logger.debug("******************* Start " + t)
        generateImage(tuple._1, t, tuple._2)
        logger.debug("*********** Finish " + t)
      })

    }
  }

  class Tile256Heatmapper(newColors: Iterable[(Double, Color)], tile: Tile) extends Heatmaper {
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
