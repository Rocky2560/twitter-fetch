package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class CreateDF {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sp: CreateSparkConnection = new CreateSparkConnection
  val sc: SparkContext = sp.spark.sparkContext
  val sqlContext: SQLContext = sp.spark.sqlContext

def json_to_df(values:String):DataFrame = {
  val rdd = sc.parallelize(Seq(values))
  val df = sqlContext.read.json(rdd)
  df
}

}
