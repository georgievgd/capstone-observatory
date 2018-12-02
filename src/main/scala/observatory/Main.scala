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

  val temperaturess: Seq[(Year, Iterable[(Location, Temperature)])] = allYears.take(3)
  val annualGrids: Seq[(Year, GridLocation => Temperature)] = temperaturess.map(tuple => (tuple._1, Manipulation.makeGrid(tuple._2)))
  Interaction.generateTiles(annualGrids, Visualization2.generateTemperaturesImage)


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


}
