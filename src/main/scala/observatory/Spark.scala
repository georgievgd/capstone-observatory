package observatory

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Spark {
  //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Observatory")
    .config("spark.sql.crossJoin.enabled", value = true)
    .getOrCreate()

  def session: SparkSession = spark

  def stop(): Unit = spark.stop()
}
