package spark

import kafka.twitter.GetProperty
import org.apache.log4j.{Level, Logger}
import testpackage.ReadJsonFile
import org.apache.spark.sql.functions._
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object TestObj {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val gp: GetProperty = new GetProperty;
    val cdf = new CreateDF
    val sp: CreateSparkConnection = new CreateSparkConnection
    val read_file = new ReadJsonFile
    val str_json = read_file.fileJson()
    val df = cdf.json_to_df(str_json.toString: String)
    val checkExist: CheckExist = new CheckExist


    //    //Read From Postgres
    //    val jdbcDF = sp.spark.read
    //      .format("jdbc")
    //      .option("url", "jdbc:postgresql://10.10.5.25:5432/twitterdb")
    //      .option("dbtable", "public.tweets_info")
    //      .option("user", "twitter")
    //      .option("password","twitter123")
    //      .load()
    //
    //    jdbcDF.show(true)
    //
    //        val c_bet = udf((value: String) => {
    //          if (value != null){
    //            value.replace("\",\"", "),(")
    //          } else null
    //        })
    //
    //    val c_start  = udf((value: String) => {
    //      if (value != null){
    //        value.replaceFirst("\"", "((")
    //      } else null
    //    })
    //
    //    val c_end = udf((value: String) => {
    //      if (value != null){
    //        value.replace("\"", "))")
    //      }
    //      else null
    //    })
    //
    //    val change_df = jdbcDF.withColumn("coordinates", c_end(c_start(c_bet(col("coordinates")))))
    //    change_df.show(true)


    //    GeoSparkSQLRegistrator.registerAll(sp.spark)
    //
    //
    //    import sp.spark.implicits._
    //
    //    //    val jsonStr = """{"coordinates": "80.063341,26.348309,80.063341,30.43339,88.2027,30.43339,88.2027,26.348309"}"""
    //    val jsonStr = """{"coordinates": "80.063341,26.348309"}"""
    //    var df = sp.spark.read.json(Seq(jsonStr).toDS)
    //    df.show()
    //
    //    df.createOrReplaceTempView("t1")
    //
    //    //    df = spark.sql("SELECT ST_LineStringFromText(coordinates, ',') AS geometry from t1")
    //    //    df = spark.sql("SELECT ST_PolygonFromText(coordinates, ',') AS geometry from t1")
    //    df = sp.spark.sql("SELECT ST_PointFromText(coordinates, ',') AS coordinates from t1")
    //    val list_df = df.collect()
    //    list_df.foreach(println)
    //    df.show(truncate = false)
    //    df.printSchema()


    val arrToString = udf((value: Seq[Seq[Double]]) => {
      //      value.map(x=> x.map(_.toString).mkString(",")).mkString("::")
      value.map(x => x.map(_.toString).mkString("\"", ",", "\"")).mkString(",")
    })

    val tweets_df =
      if (df.columns.contains("extended_tweet")){
        checkExist.extendedTweet(df)
      } else {
        checkExist.mentionsHashtags(df)
      }

    tweets_df.show(false)

    //
    //    val arrToString = udf((value: Seq[Seq[Double]]) => {
    //      //      value.map(x=> x.map(_.toString).mkString(",")).mkString("::")
    ////      value.map(x => x.map(_.toString).mkString("(", ",", ")")).mkString("(",",",")")
    //      value.map(x => x.map(_.toString).mkString("(", ",", ")")).mkString(",")
    //    })
    //
    //    val tweets_df = df1.select(col("place.bounding_box.coordinates"))
    //      .withColumn("coordinates", explode(col("coordinates"))).withColumn("coordinates", arrToString(col("coordinates")))
    //
    //    tweets_df.show(false)
    //    tweets_df.printSchema()


    //    tweets_df.write
    //      .format("jdbc")
    //      .option("url", "jdbc:postgresql://10.10.5.32:5432/twitterdb")
    //      //      .option("dbtable", gp.getPGTweetsTable)
    //      .option("dbtable", "tweets_info")
    //      .option("user", "twitter")
    //      .option("password", "twitter123")
    //      .mode("append")
    //      .save()

  }
}


//val arrToString = udf((value: Seq[Seq[Double]]) => {
//  //      value.map(x=> x.map(_.toString).mkString(",")).mkString("::")
//  value.map(x => x.map(_.toString).mkString("\"", ",", "\"")).mkString(",")
//})
//
//  val check_entities_mentions = df.select(col("entities.user_mentions").getItem(0)).collectAsList()
//  val check_entities_hashtags = df.select(col("entities.hashtags").getItem(0)).collectAsList()
//
//  val tweets_df = {
//  if (check_entities_mentions.get(0)(0) == null || check_entities_hashtags.get(0)(0) == null) {
//  if (check_entities_mentions.get(0)(0) == null && check_entities_hashtags.get(0)(0) != null) {
//  df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//  col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//  col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//  col("entities.hashtags.text").as("hashtags")).withColumn("coordinates", explode(col("coordinates"))).
//  withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
//  .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
//}
//  else if (check_entities_mentions.get(0)(0) != null && check_entities_hashtags.get(0)(0) == null) {
//  df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//  col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//  col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//  col("entities.hashtags").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
//  col("entities.user_mentions.name").as("user_mentions_name")).withColumn("coordinates", explode(col("coordinates"))).
//  withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
//  .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
//} else {
//  df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//  col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//  col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//  col("entities.hashtags").as("hashtags")).withColumn("coordinates", explode(col("coordinates"))).
//  withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
//  .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
//}
//}
//  else {
//  df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//  col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//  col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//  col("entities.hashtags.text").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
//  col("entities.user_mentions.name").as("user_mentions_name")).withColumn("coordinates", explode(col("coordinates"))).
//  withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
//  .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
//}
//}

//    val arrToString_Entites = udf((value: Seq[Seq[Double]]) => {
//      value.map(x => x.map(_.toString).mkString("\"", ",", "\"")).mkString(",")
//    })

//    val user_df = df.select("user.id", "user.name", "user.screen_name", "user.location", "user.description", "user.followers_count", "user.friends_count", "user.profile_image_url_https")
//    val json_tweets = tweets_df.na.fill("null").toJSON.collectAsList().get(0).toString
//    val json_user = user_df.na.fill("null").toJSON.collectAsList().toString




