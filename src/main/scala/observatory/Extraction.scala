package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 1st milestone: data extraction
  */
object Extraction {

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationsPath = Paths.get(getClass.getResource(stationsFile).toURI).toString
    val tempPath = Paths.get(getClass.getResource(temperaturesFile).toURI).toString
    val spark = Spark.session
    import spark.implicits._

    val stations = Spark.session.read.csv(stationsPath)
      .withColumnRenamed("_c0", "STN")
      .withColumnRenamed("_c1", "WBAN")
      //.withColumn("WBAN", when('_c1.isNull, "x"))
      .withColumn("lat", col("_c2").cast("double"))
      .withColumn("lon", col("_c3").cast("double"))
      .filter('lat.notEqual(0) && 'lon.notEqual(0))

//    stations.printSchema()
//    stations.show()
//    println(stations.count())

    val temp = Spark.session.read.csv(tempPath)
      .withColumnRenamed("_c0", "STN")
      .withColumnRenamed("_c1", "WBAN")
      //.withColumn("WBAN", when('_c1.isNull, "x"))
      .withColumn("month", col("_c2").cast("int"))
      .withColumn("day", col("_c3").cast("int"))
      .withColumn("temp", col("_c4").cast("double"))

//    temp.printSchema()
//    temp.show()
//    println(temp.count())

    val stationTemps = stations.join(temp, stations("STN") <=> temp("STN") && stations("WBAN") <=> temp("WBAN"))//Seq("STN", "WBAN"))
      .withColumn("tempC", f2c(col("temp")))
      .select("month", "day", "lat", "lon", "tempC")

//    stationTemps.printSchema()
//    stationTemps.show()
//    println(stationTemps.count())

    val result = stationTemps.collect().map{row => (
      LocalDate.of(year, row.getInt(0), row.getInt(1)),
      Location(row.getDouble(2), row.getDouble(3)),
      row.getDouble(4)
      ) }
    result
  }

  val f2c: UserDefinedFunction = udf((f: Double) => Utils.f2c(f))

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val result = records.groupBy(_._2).mapValues(list => average(list.map(_._3)))
    result
  }

  def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
    num.toDouble( ts.sum ) / ts.size
  }

}
