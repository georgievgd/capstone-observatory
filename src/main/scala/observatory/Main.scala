package observatory

import org.apache.spark.sql.SaveMode


object Main extends App {

/*  val temps = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
  val locationAverages = Extraction.locationYearlyAverageRecords(temps)
  saveLocAve(locationAverages)*/
  val averages: Iterable[(Location, Temperature)] = loadLocAve()


  val image = Visualization.visualize(averages, Utils.colorScale)
  val outputFile = new java.io.File("target/visualisation_p25/2015.png")
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
