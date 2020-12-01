//package spark
//
//import kafka.twitter.GetProperty
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.functions.{lit, month, to_timestamp, when, year}
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//
//
//object TestObj {
//  Logger.getLogger("org").setLevel(Level.ERROR)
//  Logger.getLogger("akka").setLevel(Level.ERROR)
//  val ccs: CreateSparkConnection = new CreateSparkConnection
//  val ss: SparkSession = ccs.spark
//  val gp: GetProperty = new GetProperty
//  val explodeInsert: ExplodeInsert = new ExplodeInsert
//
//  //  def TweetFromPostgres(): DataFrame = {
//  //    val tweetDF = ss.read
//  //      .format("jdbc")
//  //      .option("url", "jdbc:postgresql://10.10.5.25:5432/twitterdb")
//  //      .option("dbtable", "public.tweetsinfo")
//  //      .option("user", "twitter")
//  //      .option("password", "twitter123")
//  //      .load()
//  //    tweetDF
//  //  }
//  //
//  //  def UserFromPostgres(): DataFrame = {
//  //    val userDF = ss.read
//  //      .format("jdbc")
//  //      .option("url", "jdbc:postgresql://10.10.5.25:5432/twitterdb")
//  //      .option("dbtable", "public.userinfo")
//  //      .option("user", "twitter")
//  //      .option("password", "twitter123")
//  //      .load()
//  //    userDF
//  //  }
//
//
//  //  //***************************** Tweets and User from Cassandra*****************************************
//  //  def TweetDf(): DataFrame = {
//  //    var tweetdf: DataFrame = ss.read.format("org.apache.spark.sql.cassandra").
//  //      options(Map("table" -> gp.getCassInsert_FromTweetsTable, "keyspace" -> "twitterdb")).
//  //      load()
//  //    import ss.implicits._
//  //    import org.apache.spark.sql.functions.when
//  //    //Set Default Values for Null Fields //Why? Because country and lang is set as parition key in some tables and primary key cant be null
//  //    tweetdf = tweetdf.withColumn("country", col = when($"country".isNotNull, $"country").otherwise("N/A"))
//  //    tweetdf = tweetdf.withColumn("lang", col = when($"lang".isNotNull, $"lang").otherwise("N/A"))
//  //    tweetdf
//  //  }
//  //
//  //  def UserDf(): DataFrame = {
//  //    val userdf: DataFrame = ss.read.format("org.apache.spark.sql.cassandra").
//  //      options(Map("table" -> gp.getCassInsert_FromUserTable, "keyspace" -> "twitterdb")).
//  //      load()
//  //    userdf
//  //  }
//
//    def Analytic(): DataFrame = {
//      var df: DataFrame = ss.read.format("org.apache.spark.sql.cassandra").
//        options(Map("table" -> "analytic", "keyspace" -> "ratopanda")).
//        load()
////      import ss.implicits._
////      import org.apache.spark.sql.functions.when
////      //Set Default Values for Null Fields //Why? Because country and lang is set as parition key in some tables and primary key cant be null
////      tweetdf = tweetdf.withColumn("country", col = when($"country".isNotNull, $"country").otherwise("N/A"))
////      tweetdf = tweetdf.withColumn("lang", col = when($"lang".isNotNull, $"lang").otherwise("N/A"))
//      df
//    }
//
//
//  def InsertData(): Unit = {
//    val df = Analytic()
//    Insert_Tweets_User_By_Date_Client(df)
//  }
//
//
//  //4
//  def Insert_Tweets_User_By_Date_Client(df: DataFrame): Unit = {
//    var final_df = df
//
//    //Year and Month partition key
//    import org.apache.spark.sql.functions._
//    import ss.implicits._
//    final_df = final_df.withColumn("created_year", year(to_timestamp($"createdat", "yyyy-MM-dd HH:mm:ss")))
//    final_df = final_df.withColumn("created_month", month(to_timestamp($"createdat", "yyyy-MM-dd HH:mm:ss")))
//
//    final_df.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append)
//      .options(Map("table" -> "analytic_by_date", "keyspace" -> "ratopanda")).save()
//    //      .options(Map("table" -> "test_tweets_user_by_date_client", "keyspace" -> "twitterdb")).save()
//  }
//
//  def main(args: Array[String]): Unit = {
//    InsertData()
//  }
//}
