package spark

import org.apache.spark.sql.SparkSession

object dumpcassandra {
  def main(args: Array[String]): Unit = {
    val ccs: CreateSparkConnection = new CreateSparkConnection
    val ss: SparkSession = ccs.spark

    var df = ss.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "tweetsinfo", "keyspace" -> "twitterdb"))
      .load()
    //      .filter("key='YOUR_KEY'")


    df = df.filter(df("created_at") >= "2021-03-16")
    df = df.filter(df("lang") === "en")
    //    df.show()
    //    df.printSchema()
    //
    //    import org.apache.spark.sql.functions._
    //    val stringify = udf((vs: Seq[String]) => vs match {
    //      case null => null
    //      case _    => s"""[${vs.mkString(",")}]"""
    //    })
    //
    //    import ss.implicits._
    //    df = df.withColumn("user_mentions_id", stringify($"user_mentions_id"))
    //    df = df.withColumn("user_mentions_name", stringify($"user_mentions_name"))
    //    df = df.withColumn("hashtags", stringify($"hashtags"))


    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/home/rocky/testdump1.csv")

  }
}
