package spark

import kafka.twitter.GetProperty
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.functions._
import testpackage.ReadJsonFile


class ExplodeInsert {
  val cdf = new CreateDF
  val gp: GetProperty = new GetProperty;
  val sp: CreateSparkConnection = new CreateSparkConnection;
  val dc: DataCleaning = new DataCleaning;
  val checkExist: CheckExist = new CheckExist

  def convertStr(msg: String): String = {
    val df = userInfo(msg)
    val json_user = df.na.fill("").toJSON.collectAsList().get(0).toString
    json_user
  }

  def convertStrTweets(msg: String): String = {
    val df = tweetsInfo(msg)
    val json_tweets = df.na.fill("").toJSON.collectAsList().get(0).toString
    json_tweets
  }

  def userInfo(msg: String): DataFrame = {
    val df: DataFrame = cdf.json_to_df(msg: String)
    val user_df_temp = df.select("user.id", "user.name", "user.screen_name", "user.location", "user.description", "user.followers_count", "user.friends_count", "user.profile_image_url_https")
    val user_df = user_df_temp.withColumn("location", when(!isnull(col("location")), dc.LocationCleaning(user_df_temp)))
    user_df
  }

  def tweetsInfo(msg: String): DataFrame = {
    val df: DataFrame = cdf.json_to_df(msg: String)

//    val arrToString = udf((value: Seq[Seq[Double]]) => {
//      value.map(x => x.map(_.toString).mkString("\"", ",", "\"")).mkString(",")
//    })

    val tweets_df =
      if (df.columns.contains("extended_tweet")){
        checkExist.extendedTweet(df)
      } else {
        checkExist.mentionsHashtags(df)
      }

    tweets_df
  }

  def InsertTweets(msg: String) = {
    val df: DataFrame = tweetsInfo(msg)
//    df.show(truncate = false)
//    df.printSchema()
    df.write
      .format("jdbc")
      .option("url", gp.getPGUrl)
      .option("dbtable", "public." + gp.getPGTweetsTable)
      .option("user", gp.getPGUsername)
      .option("password", gp.getPGPassword)
      .mode("append")
      .save()
  }

  def InsertUserInfo(msg: String) = {
    val df: DataFrame = SpecificUser(msg)
    df.write
      .format("jdbc")
      .option("url", gp.getPGUrl)
      .option("dbtable", gp.getPGUserTable)
      .option("user", gp.getPGUsername)
      .option("password", gp.getPGPassword)
      .mode("append")
      .save()
  }


  //Tweets not from Nepal
  def SpecficInsert(msg: String) = {
    val df: DataFrame = SpecificTweets(msg)
    df.write
      .format("jdbc")
      .option("url", gp.getPGUrl)
      .option("dbtable", "public." + gp.getPGTweetsTable)
      .option("user", gp.getPGUsername)
      .option("password", gp.getPGPassword)
      .mode("append")
      .save()
  }

  def SpecificUser(msg: String): DataFrame = {
    val df: DataFrame = cdf.json_to_df(msg: String)
    val user_df_temp = df.select("user.id", "user.name", "user.screen_name", "user.location", "user.description", "user.followers_count", "user.friends_count", "user.profile_image_url_https")
//    val user_df = user_df_temp.withColumn("location", when(!isnull(col("location")), dc.LocationCleaning(user_df_temp)))
    user_df_temp
  }


  def SpecificTweets(msg:String):DataFrame = {

    val df: DataFrame = cdf.json_to_df(msg: String)

    val check_entities_mentions = df.select(col("entities.user_mentions").getItem(0)).collectAsList()
    val check_entities_hashtags = df.select(col("entities.hashtags").getItem(0)).collectAsList()

    val tweets_df = {
      if (check_entities_mentions.get(0)(0) == null || check_entities_hashtags.get(0)(0) == null) {
        if (check_entities_mentions.get(0)(0) == null && check_entities_hashtags.get(0)(0) != null) {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"),
            col("entities.hashtags.text").as("hashtags")).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        }
        else if (check_entities_mentions.get(0)(0) != null && check_entities_hashtags.get(0)(0) == null) {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"),
            col("entities.hashtags").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
            col("entities.user_mentions.name").as("user_mentions_name")).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        } else {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"),
            col("entities.hashtags").as("hashtags")).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        }
      }
      else {
        df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
          col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
          col("lang"), col("retweet_count"), col("reply_count"),
          col("entities.hashtags.text").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
          col("entities.user_mentions.name").as("user_mentions_name")).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
          .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
      }
    }
    tweets_df
  }
}

