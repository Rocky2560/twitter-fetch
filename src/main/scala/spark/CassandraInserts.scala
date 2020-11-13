package spark

import kafka.twitter.GetProperty
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{lit, month, to_timestamp, when, year}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class CassandraInserts {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val ccs: CreateSparkConnection = new CreateSparkConnection
  val ss: SparkSession = ccs.spark
  val gp: GetProperty = new GetProperty
  val explodeInsert: ExplodeInsert = new ExplodeInsert

  //  def TweetFromPostgres(): DataFrame = {
  //    val tweetDF = ss.read
  //      .format("jdbc")
  //      .option("url", "jdbc:postgresql://10.10.5.25:5432/twitterdb")
  //      .option("dbtable", "public.tweetsinfo")
  //      .option("user", "twitter")
  //      .option("password", "twitter123")
  //      .load()
  //    tweetDF
  //  }
  //
  //  def UserFromPostgres(): DataFrame = {
  //    val userDF = ss.read
  //      .format("jdbc")
  //      .option("url", "jdbc:postgresql://10.10.5.25:5432/twitterdb")
  //      .option("dbtable", "public.userinfo")
  //      .option("user", "twitter")
  //      .option("password", "twitter123")
  //      .load()
  //    userDF
  //  }


  //  //***************************** Tweets and User from Cassandra*****************************************
  //  def TweetDf(): DataFrame = {
  //    var tweetdf: DataFrame = ss.read.format("org.apache.spark.sql.cassandra").
  //      options(Map("table" -> gp.getCassInsert_FromTweetsTable, "keyspace" -> "twitterdb")).
  //      load()
  //    import ss.implicits._
  //    import org.apache.spark.sql.functions.when
  //    //Set Default Values for Null Fields //Why? Because country and lang is set as parition key in some tables and primary key cant be null
  //    tweetdf = tweetdf.withColumn("country", col = when($"country".isNotNull, $"country").otherwise("N/A"))
  //    tweetdf = tweetdf.withColumn("lang", col = when($"lang".isNotNull, $"lang").otherwise("N/A"))
  //    tweetdf
  //  }
  //
  //  def UserDf(): DataFrame = {
  //    val userdf: DataFrame = ss.read.format("org.apache.spark.sql.cassandra").
  //      options(Map("table" -> gp.getCassInsert_FromUserTable, "keyspace" -> "twitterdb")).
  //      load()
  //    userdf
  //  }

  def TweetDf(msg: String): DataFrame = {
    var tweetsinfo = explodeInsert.tweetsInfo(msg)
    import ss.implicits._
    import org.apache.spark.sql.functions.when
    //Set Default Values for Null Fields //Why? Because country and lang is set as parition key in some tables and primary key cant be null
    if (tweetsinfo.columns.contains("country")) {
      tweetsinfo = tweetsinfo.withColumn("country", col = when($"country".isNotNull, $"country").otherwise("N/A"))
    } else {
      tweetsinfo = tweetsinfo.withColumn("country", lit("N/A"))
    }
    tweetsinfo = tweetsinfo.withColumn("lang", col = when($"lang".isNotNull, $"lang").otherwise("N/A"))
    tweetsinfo
  }

  def SpecificTweetDf(msg: String): DataFrame = {
    var tweetsinfo = explodeInsert.SpecificTweets(msg)
    import ss.implicits._
    import org.apache.spark.sql.functions.when
    //Set Default Values for Null Fields //Why? Because country and lang is set as parition key in some tables and primary key cant be null
    if (tweetsinfo.columns.contains("country")) {
      tweetsinfo = tweetsinfo.withColumn("country", col = when($"country".isNotNull, $"country").otherwise("N/A"))
    } else {
      tweetsinfo = tweetsinfo.withColumn("country", lit("N/A"))
    }
    tweetsinfo = tweetsinfo.withColumn("lang", col = when($"lang".isNotNull, $"lang").otherwise("N/A"))
    tweetsinfo
  }

  def UserDf(msg: String): DataFrame = {
    val userinfo = explodeInsert.userInfo(msg)
    userinfo
  }


  def InsertData(msg: String): Unit = {
    val tweetsinfo = TweetDf(msg)
    val userinfo = UserDf(msg)
    val tweet_user_date_df = Tweets_User_Date(tweetsinfo, userinfo)

    Insert_Tweets_User_By_Date_Client(tweet_user_date_df)
    Insert_Tweets_User_By_Country_Date(tweet_user_date_df)

    Insert_TweetsInfo(tweetsinfo)
    Insert_UserInfo(userinfo)
    Insert_Tweets_By_Lang(tweetsinfo)

  }

  def SpecificInsertData(msg: String): Unit = {
    //Specific tweets (Bhannale Nepal bhanda baira ko tweets)
    val tweetsinfo = SpecificTweetDf(msg)
    val userinfo = UserDf(msg)
    val tweet_user_date_df = Tweets_User_Date(tweetsinfo, userinfo)

    Insert_Tweets_User_By_Date_Client(tweet_user_date_df)
    Insert_Tweets_User_By_Country_Date(tweet_user_date_df)


    Insert_TweetsInfo(tweetsinfo)
    Insert_UserInfo(userinfo)
    Insert_Tweets_By_Lang(tweetsinfo)

  }

  //1
  def Insert_TweetsInfo(tweetdf: DataFrame): Unit = {
    tweetdf.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
      .options(Map("table" -> "tweetsinfo", "keyspace" -> "twitterdb")).save()
  }

  //2
  def Insert_UserInfo(userdf: DataFrame): Unit = {
    userdf.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
      .options(Map("table" -> "userinfo", "keyspace" -> "twitterdb")).save()
  }

  //3
  def Insert_Tweets_By_Lang(tweetdf: DataFrame): Unit = {
    tweetdf.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
      .options(Map("table" -> "tweets_by_lang", "keyspace" -> "twitterdb")).save()
  }

  def Tweets_User_Date(tdf: DataFrame, udf: DataFrame): DataFrame = {
    val tweetdf = tdf.as("tweetdf")
    var userdf = udf

    userdf = userdf.withColumnRenamed("id", "userid").as("userdf")

    import ss.implicits._
    val final_df = userdf.join(tweetdf, $"tweetdf.user_id" === $"userdf.userid").drop("userid")
//    final_df.show(5, truncate = true)

    final_df
  }

  //4
  def Insert_Tweets_User_By_Date_Client(df: DataFrame): Unit = {
    var final_df = df

    //Year and Month partition key
    import org.apache.spark.sql.functions._
    import ss.implicits._
    final_df = final_df.withColumn("created_year", year(to_timestamp($"created_at", "yyyy-MM-dd HH:mm:ss")))
    final_df = final_df.withColumn("created_month", month(to_timestamp($"created_at", "yyyy-MM-dd HH:mm:ss")))

    final_df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
      .options(Map("table" -> "tweets_user_by_date_client", "keyspace" -> "twitterdb")).save()
    //      .options(Map("table" -> "test_tweets_user_by_date_client", "keyspace" -> "twitterdb")).save()
  }

  //5
  def Insert_Tweets_User_By_Country_Date(final_df: DataFrame): Unit = {
    final_df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
      .options(Map("table" -> "tweets_user_by_country_date", "keyspace" -> "twitterdb")).save()
  }

  //6
  def Insert_Tweets_By_Country(tweetdf: DataFrame): Unit = {
    tweetdf.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
      .options(Map("table" -> "tweets_by_country", "keyspace" -> "twitterdb")).save()
  }


}
