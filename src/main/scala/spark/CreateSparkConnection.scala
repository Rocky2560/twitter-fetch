package spark

import kafka.twitter.GetProperty
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


class CreateSparkConnection {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

//  val gfp:GetProperty=new GetProperty()

  val spark:SparkSession = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "100")
    .appName("Kafka Twitter")
    .master("local")
    .getOrCreate()

}

