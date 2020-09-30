package spark

import kafka.twitter.GetProperty
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


class CreateSparkConnection {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val gp:GetProperty=new GetProperty()

  val spark:SparkSession = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "100")

    .config("spark.debug.maxToStringFields", "100")
    .config("spark.cassandra.connection.host", gp.getHost)
    .config("spark.cassandra.connection.port", gp.getPort)
    .config("spark.cassandra.auth.username", gp.getCassUser)
    .config("spark.cassandra.auth.password", gp.getCassPass)
    .config("spark.cassandra.connection.ssl.clientAuth.enabled", value = true)
    .config("spark.cassandra.connection.ssl.enabled", value = true)
    .config("spark.cassandra.connection.ssl.keyStore.path", gp.getKeystore_path)
    .config("spark.cassandra.connection.ssl.keyStore.password", gp.getKeystore_password)
    .config("spark.cassandra.connection.ssl.trustStore.path", gp.getTruststore_path)
    .config("spark.cassandra.connection.ssl.trustStore.password", gp.getTruststore_password)
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.yarn.executor.memoryOverhead", "4g")


    .appName("Kafka Twitter")
    .master("local")
    .getOrCreate()

}

