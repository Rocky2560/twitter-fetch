package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, to_timestamp, to_utc_timestamp, udf}

class CheckExist {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val arrToString = udf((value: Seq[Seq[Double]]) => {
    value.map(x => x.map(_.toString).mkString("(", ",", ")")).mkString("(",",",")")
  })

  def mentionsHashtags(df:DataFrame): DataFrame = {
    val check_entities_mentions = df.select(col("entities.user_mentions").getItem(0)).collectAsList()
    val check_entities_hashtags = df.select(col("entities.hashtags").getItem(0)).collectAsList()

    val tweets_df = {
      if (check_entities_mentions.get(0)(0) == null || check_entities_hashtags.get(0)(0) == null) {
        if (check_entities_mentions.get(0)(0) == null && check_entities_hashtags.get(0)(0) != null) {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags.text").as("hashtags"), col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        }
        else if (check_entities_mentions.get(0)(0) != null && check_entities_hashtags.get(0)(0) == null) {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
            col("entities.user_mentions.name").as("user_mentions_name"), col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        } else {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags").as("hashtags"), col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        }
      }
      else {
        df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
          col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
          col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
          col("entities.hashtags.text").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
          col("entities.user_mentions.name").as("user_mentions_name"), col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
          withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
          .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
      }
    }
    tweets_df
  }


  def extendedTweet(df:DataFrame): DataFrame = {
    val check_entities_mentions = df.select(col("entities.user_mentions").getItem(0)).collectAsList()
    val check_entities_hashtags = df.select(col("entities.hashtags").getItem(0)).collectAsList()

    val tweets_df = {
      if (check_entities_mentions.get(0)(0) == null || check_entities_hashtags.get(0)(0) == null) {
        if (check_entities_mentions.get(0)(0) == null && check_entities_hashtags.get(0)(0) != null) {
          df.select(col("created_at"), col("id"), col("extended_tweet.full_text").as("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags.text").as("hashtags"), col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        }
        else if (check_entities_mentions.get(0)(0) != null && check_entities_hashtags.get(0)(0) == null) {
          df.select(col("created_at"), col("id"), col("extended_tweet.full_text").as("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
            col("entities.user_mentions.name").as("user_mentions_name"),col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        } else {
          df.select(col("created_at"), col("id"), col("extended_tweet.full_text").as("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags").as("hashtags"), col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
            .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
        }
      }
      else {
        df.select(col("created_at"), col("id"), col("extended_tweet.full_text").as("text"), col("source"), col("user.id").as("user_id"),
          col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
          col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
          col("entities.hashtags.text").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
          col("entities.user_mentions.name").as("user_mentions_name"), col("place.country")).withColumn("coordinates", explode(col("coordinates"))).
          withColumn("coordinates", arrToString(col("coordinates"))).withColumn("created_at", to_timestamp(col("created_at"), "EEE MMM d HH:mm:ss z yyyy"))
          .withColumn("created_at", to_utc_timestamp(col("created_at"), "Asia/Kathmandu"))
      }
    }
    tweets_df
  }

}
