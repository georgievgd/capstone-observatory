package observatory

import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

import scala.collection.immutable


object Main extends App {
  val logger = Logger.getLogger(this.getClass)

//  for(i <- 1975 to 2015){
//    val temps = Extraction.locateTemperatures(i, "/stations.csv", "/" + i + ".csv")
//    val locationAverages = Extraction.locationYearlyAverageRecords(temps)
//    saveLocAve(i,locationAverages)
//  }

  val allYears: Seq[(Year, Iterable[(Location, Temperature)])] = for(i <- 1975 to 2015) yield (i,loadLocAve(i))
  logger.debug("Temperaturess size is " + allYears.size)
  logger.debug("Temps for " + allYears.head._1 + " - " + allYears.head._2.size)
  val temperaturess: Seq[(Year, Iterable[(Location, Temperature)])] = allYears

  //Generate temperature grids
//  val annualGrids: Seq[(Year, GridLocation => Temperature)] = temperaturess.map(tuple => {
//    logger.debug("Start grid for " + tuple._1)
//    val grid: GridLocation => Temperature = Manipulation.makeGrid(tuple._2)
//    logger.debug("End grid for " + tuple._1)
//    saveGrid("grid/" + tuple._1 + "/", grid)
//    logger.debug("Saved grid for " + tuple._1)
//    logger.debug("Now sleep for 10 seconds")
//    Thread.sleep(10000)
//    (tuple._1, grid)
//  })

  // Generate temperatures images for all years
  //val annualGrids: Seq[(Year, GridLocation => Temperature)] = for(i <- 1975 to 2015) yield (i,loadGrid("grid/" + i))
//  Interaction.generateTiles(annualGrids, Visualization2.generateTemperaturesImage)

  //Generate deviation grids
//  logger.debug("Start normal grid")
//  val normalGrid: GridLocation => Temperature = Manipulation.averageGrids(annualGrids.map(_._2))//average(temperaturess.map(_._2))
//  logger.debug("End normal grid")
//  val deviationGrids: Seq[(Year, GridLocation => Temperature)] = temperaturess.map(tuple => {
//    logger.debug("Start deviation grid for " + tuple._1)
//    val deviationGrid: GridLocation => Temperature = Manipulation.deviation(tuple._2, normalGrid)
//    logger.debug("End deviation grid for " + tuple._1)
//    saveGrid("deviation-grid/" + tuple._1 + "/", deviationGrid)
//    logger.debug("Saved deviation grid for " + tuple._1)
//    logger.debug("Now sleep for 10 seconds")
//    Thread.sleep(10000)
//    (tuple._1, deviationGrid)
//  })

  //Generate deviation images for all years
  val deviationGrids: Seq[(Year, GridLocation => Temperature)] = for(i <- 1975 to 2015) yield (i,loadGrid("deviation-grid/" + i))
  Interaction.generateTiles(deviationGrids, Visualization2.generateDeviationsImage)




  def saveLocAve(year: Year, temps: Iterable[(Location, Temperature)]): Unit = {
    val spark = Spark.session
    import spark.implicits._

    temps.toSeq.toDF
      .write.mode(SaveMode.Overwrite)
      .parquet("target/avetemps/" + year)
  }

  def loadLocAve(year: Year): Iterable[(Location, Temperature)] = {
    val spark = Spark.session
    import spark.implicits._

    val df = spark.read.parquet("target/avetemps/" + year).as[(Location, Temperature)]
    //df.printSchema()
    df.collect()

  }

  def saveGrid(path:String, grid: GridLocation => Temperature): Unit = {
    val spark = Spark.session
    import spark.implicits._

    val gridLocations: Seq[GridLocation] =
      for{
        lat <- -89 to 90
        lon <- -180 to 179
      } yield GridLocation(lat, lon)

    val gridList = gridLocations.map(gl => (gl, grid(gl)))
    gridList.toDF
      .write.mode(SaveMode.Overwrite)
      .parquet("target/" + path)
  }

  def loadGrid(path: String): GridLocation => Temperature = {
    val spark = Spark.session
    import spark.implicits._

    val df = spark.read.parquet("target/" + path).as[(GridLocation, Temperature)]
    val grid: Map[GridLocation, Temperature] = df.collect().toMap
    (gLocation => grid(gLocation))
  }




  //  val image = Visualization.visualize(averages, Utils.colorScale)
  //  val outputFile = new java.io.File("target/visualisation_p25/2015.png")
  //  import java.nio.file.Files
  //  val parentDir = outputFile.toPath.getParent
  //  if (!Files.exists(parentDir)) Files.createDirectories(parentDir)
  //  image.output(outputFile)

  //Interaction.generateTiles(Seq((2015,averages)), Interaction.generateImage)

  //Spark.stop()

  //  val grid: GridLocation => Temperature = Manipulation.makeGrid(averages)
  //  logger.debug("Grid complete")
  //  val t1: Temperature = grid(GridLocation(0,0))
  //  logger.debug("Temperature for 0,0 is " + t1)
  //  val t2: Temperature = grid(GridLocation(90, -180))
  //  logger.debug("Temperature for 90,-180 is " + t2)
  //
  //  val mapper = new GlobalHeatmapper()
  //  val image = mapper.buildGridImage(grid)
  //  val outputFile = new java.io.File("target/visualisation2/2015.png")
  //  import java.nio.file.Files
  //  val parentDir = outputFile.toPath.getParent
  //  if (!Files.exists(parentDir)) Files.createDirectories(parentDir)
  //  image.output(outputFile)
}
