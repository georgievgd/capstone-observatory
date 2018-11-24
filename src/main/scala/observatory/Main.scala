package observatory

import org.apache.spark.sql.SaveMode


object Main extends App {

/*  val temps = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
  val locationAverages = Extraction.locationYearlyAverageRecords(temps)
  saveLocAve(locationAverages)*/
  val averages: Iterable[(Location, Temperature)] = loadLocAve()

/*  val loc = Location(+33.450,-105.517)
  val predictedTemp = Visualization.predictTemperature(averages, loc)
  println(s"Predicted temp = ${predictedTemp}")

  val loc2 = Location(+33.451,-105.517)
  val predictedTemp2 = Visualization.predictTemperature(averages, loc2)
  println(s"Predicted temp2 = ${predictedTemp2}")

  val loc3 = Location(88.0,-176.0)
  val predictedTemp3 = Visualization.predictTemperature(averages, loc3)
  println(s"Predicted temp3 = ${predictedTemp3}")

  val loc4 = Location(89.0,-180.0)
  val predictedTemp4 = Visualization.predictTemperature(averages, loc4)
  println(s"Predicted temp4 = ${predictedTemp4}")

  val color = Visualization.interpolateColor(Utils.colorScale, predictedTemp4)
  println(s"Color at location4 - ${color}")*/

  val image = Visualization.visualize(averages, Utils.colorScale)
  val outputFile = new java.io.File("target/visualisation_v4/2015.png")
  import java.nio.file.Files
  val parentDir = outputFile.toPath.getParent
  if (!Files.exists(parentDir)) Files.createDirectories(parentDir)
  image.output(outputFile)
  println("All done")


  //Interaction.generateTiles(Seq((2015,averages)), Interaction.generateImage)

  //Spark.stop()


  def saveLocAve(temps: Iterable[(Location, Temperature)]): Unit = {
    val spark = Spark.session
    import spark.implicits._

    temps.toSeq.toDF
      .write.mode(SaveMode.Overwrite)
      .parquet("C:/Development/workspace/output/cache/avetemps")
  }

  def loadLocAve(): Iterable[(Location, Temperature)] = {
    val spark = Spark.session
    import spark.implicits._

    val df = spark.read.parquet("C:/Development/workspace/output/cache/avetemps").as[(Location, Temperature)]
    //df.printSchema()
    df.collect()

  }


}
