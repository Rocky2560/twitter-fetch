package spark

import com.google.gson.JsonObject
import kafka.twitter.GetProperty
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Encoders}
import org.json.JSONObject
//import testpackage.ReadJsonFile
import org.apache.spark.sql.functions._


class ExplodeInsert {
  val cdf = new CreateDF
//  val read_file = new ReadJsonFile
//  val str_json: JsonObject = read_file.fileJson()
//  val df: DataFrame = cdf.json_to_df(str_json.toString: String)
  val gp:GetProperty = new GetProperty;

  def userInfo(msg: String): DataFrame = {
    val df: DataFrame = cdf.json_to_df(msg: String)

    val user_df = df.select("user.id", "user.name", "user.screen_name", "user.location", "user.description", "user.followers_count", "user.friends_count", "user.profile_image_url_https")
    user_df
//    val json_user = user_df.na.fill("null").toJSON.collectAsList().get(0).toString
//    json_user
  }

  def convertStr(msg:String):String ={
    val df = userInfo(msg)
    val json_user = df.na.fill("").toJSON.collectAsList().get(0).toString
    json_user
  }

  def tweetsInfo(msg: String): DataFrame = {
    val df: DataFrame = cdf.json_to_df(msg: String)

    val arrToString = udf((value: Seq[Seq[Double]]) => {
      value.map(x=> x.map(_.toString).mkString("\"",  "," , "\"")).mkString(",")
    })

    val tweets_df = df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
      col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
      col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
      col("entities").cast("String")).withColumn("coordinates", explode(col("coordinates"))).
      withColumn("coordinates", arrToString(col("coordinates")))

//    val tweets_df = df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//      col("in_reply_to_status_id_str"), col("in_reply_to_user_id_str"),
//      col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//      col("entities.hashtags"), col("entities.user_mentions.id_str").as("user_mentions_id"),
//      col("entities.user_mentions.name").as("user_mentions_name"),
//      col("entities.user_mentions.screen_name").as("user_mentions_screen_name")).withColumn("coordinates", explode(col("coordinates"))).
//      withColumn("coordinates", arrToString(col("coordinates")))

    tweets_df
//    val json_tweets = tweets_df.na.fill("null").toJSON.collectAsList.get(0).toString
//    InsertToPostgres(tweets_df)

//    json_tweets
  }

  def InsertTweets(msg: String) = {
    val df:DataFrame = tweetsInfo(msg)
    df.write
      .format("jdbc")
      .option("url", gp.getPGUrl)
      .option("dbtable", "public."+gp.getPGTweetsTable)
      .option("user", gp.getPGUsername)
      .option("password", gp.getPGPassword)
      .mode("append")
      .save()
  }

  def InsertUserInfo(msg: String) = {
    val df:DataFrame = userInfo(msg)
    df.write
      .format("jdbc")
      .option("url", gp.getPGUrl)
      .option("dbtable","public."+gp.getPGUserTable)
      .option("user", gp.getPGUsername)
      .option("password", gp.getPGPassword)
      .mode("append")
      .save()
  }

}














//object ExplodeTest {
//
//  Logger.getLogger("org").setLevel(Level.ERROR)
//  Logger.getLogger("akka").setLevel(Level.ERROR)
//
//  def main(args: Array[String]): Unit = {
//    val cdf = new CreateDF
//    val read_file = new ReadJsonFile
//    val str_json = read_file.fileJson()
//    val df = cdf.json_to_df(str_json.toString: String)
//    //    df.show(truncate = false)
//    //    df.printSchema()
//
//    //    val place_df = df.withColumn("place", explodeDF("place"))
//    //    val place_df = df.select("place.bounding_box.coordinates")
//    //    place_df.show(truncate = false)
//
//
//    //    val initial_tweets_df = df.select(col("created_at"), "id", "text", "source","user.id", "in_reply_to_status_id", "in_reply_to_user_id", "lang", "retweet_count",
//    //      "reply_count", "place.bounding_box.coordinates", "entities.hashtags", "entities.user_mentions")
//
//
//    val arrToString = udf((value: Seq[Seq[Double]]) => {
////      value.map(x=> x.map(_.toString).mkString(",")).mkString("::")
//      value.map(x=> x.map(_.toString).mkString("\"",  "," , "\"")).mkString(",")
//    })
//
////    val tweets_df = df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
////      col("in_reply_to_status_id"), col("in_reply_to_user_id"),
////      col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
////      col("entities.hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
////      col("entities.user_mentions.name").as("user_mentions_name")).withColumn("coordinates", explode(col("coordinates"))).
////      withColumn("coordinates", arrToString(col("coordinates")))
//
//
//    val arrToString_Entites = udf((value: Seq[Seq[Double]]) => {
//      value.map(x=> x.map(_.toString).mkString("\"",  "," , "\"")).mkString(",")
//    })
//
//
//    val tweets_df = df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//      col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//      col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//      col("entities").cast("String")).withColumn("coordinates", explode(col("coordinates"))).
//      withColumn("coordinates", arrToString(col("coordinates")))
//
//
//    val user_df = df.select("user.id", "user.name", "user.screen_name", "user.location", "user.description", "user.followers_count", "user.friends_count", "user.profile_image_url_https")
//
//    tweets_df.show(truncate = false)
//    tweets_df.printSchema()
////    tweets_df.printSchema()
////        user_df.show(truncate = true)
//
////    val json_tweets = tweets_df.na.fill("null").toJSON.collectAsList().get(0).toString
////    val json_user = user_df.na.fill("null").toJSON.collectAsList().toString
////    println(json_tweets)
//
//    tweets_df.write
//      .format("jdbc")
//      .option("url", "jdbc:postgresql://10.10.5.32:5432/twitterdb")
//      .option("dbtable", "public.tweetsinfo3")
//      .option("user", "twitter")
//      .option("password", "twitter123")
//      .mode("append")
//      .save()
//
//    //    println(json_user)
//
//    //    val json_tweets = cdf.sqlContext.read().json(tweets_df.toJSON())
//
//  }
//}
