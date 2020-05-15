package spark

import kafka.twitter.GetProperty
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.functions._
import testpackage.ReadJsonFile


class ExplodeInsert {
  val cdf = new CreateDF
  val gp: GetProperty = new GetProperty;
  val sp: CreateSparkConnection = new CreateSparkConnection;


  def userInfo(msg: String): DataFrame = {
    val df: DataFrame = cdf.json_to_df(msg: String)
    println(msg)
    val user_df_temp = df.select("user.id", "user.name", "user.screen_name", "user.location", "user.description", "user.followers_count", "user.friends_count", "user.profile_image_url_https")
    val user_df = user_df_temp.withColumn("location", when(!isnull(col("location")), LocationCleaning(user_df_temp)))
    user_df
  }

  def convertStr(msg: String): String = {
    val df = userInfo(msg)
    val json_user = df.na.fill("").toJSON.collectAsList().get(0).toString
    json_user
  }

  def tweetsInfo(msg: String): DataFrame = {
    val df: DataFrame = cdf.json_to_df(msg: String)

    val arrToString = udf((value: Seq[Seq[Double]]) => {
      value.map(x => x.map(_.toString).mkString("\"", ",", "\"")).mkString(",")
    })

    val check_entities_mentions = df.select(col("entities.user_mentions").getItem(0)).collectAsList()
    val check_entities_hashtags = df.select(col("entities.hashtags").getItem(0)).collectAsList()

    val tweets_df = {
      if (check_entities_mentions.get(0)(0) == null || check_entities_hashtags.get(0)(0) == null) {
        if (check_entities_mentions.get(0)(0) == null && check_entities_hashtags.get(0)(0) != null) {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags.text").as("hashtags")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates")))
        }
        else if (check_entities_mentions.get(0)(0) != null && check_entities_hashtags.get(0)(0) == null) {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
            col("entities.user_mentions.name").as("user_mentions_name")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates")))
        } else {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
            col("entities.hashtags").as("hashtags")).withColumn("coordinates", explode(col("coordinates"))).
            withColumn("coordinates", arrToString(col("coordinates")))
        }
      }
      else {
        df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
          col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
          col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
          col("entities.hashtags.text").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
          col("entities.user_mentions.name").as("user_mentions_name")).withColumn("coordinates", explode(col("coordinates"))).
          withColumn("coordinates", arrToString(col("coordinates")))
      }
    }
    tweets_df
  }

  def InsertTweets(msg: String) = {
    val df: DataFrame = tweetsInfo(msg)
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
    val df: DataFrame = userInfo(msg)
    df.write
      .format("jdbc")
      .option("url", gp.getPGUrl)
      .option("dbtable", gp.getPGUserTable)
      .option("user", gp.getPGUsername)
      .option("password", gp.getPGPassword)
      .mode("append")
      .save()
  }

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
            col("entities.hashtags.text").as("hashtags"))
        }
        else if (check_entities_mentions.get(0)(0) != null && check_entities_hashtags.get(0)(0) == null) {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"),
            col("entities.hashtags").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
            col("entities.user_mentions.name").as("user_mentions_name"))
        } else {
          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
            col("lang"), col("retweet_count"), col("reply_count"),
            col("entities.hashtags").as("hashtags"))
        }
      }
      else {
        df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
          col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
          col("lang"), col("retweet_count"), col("reply_count"),
          col("entities.hashtags.text").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
          col("entities.user_mentions.name").as("user_mentions_name"))
      }
    }
    tweets_df

  }

  def LocationCleaning(user_df: DataFrame): String = {
//    val city_df1 = sp.spark.read.format("csv").option("header", "true").load("/Users/tchiringlama/nepali_cities.csv")
    val city_df1 = sp.spark.read.format("csv").option("header", "true").load(gp.getCity())
    val city_df = city_df1.select(city_df1.columns.map(c => lower(col(c)).alias(c)): _*)

    val countries_list = List("united arab emirates", "nigeria", "ghana", "pitcairn islands", "ethiopia", "algeria", "niue",
      "jordan", "netherlands", "andorra", "turkey", "madagascar", "samoa", "turkmenistan",
      "eritrea",
      "paraguay", "greece", "cook islands", "iraq", "azerbaijan", "mali", "brunei", "thailand",
      "central african republic", "gambia", "saint kitts and nevis", "china", "lebanon", "serbia",
      "belize", "germany", "switzerland", "kyrgyzstan", "guinea-bissau", "colombia", "brazil",
      "slovakia", "republic of the congo", "barbados", "belgium", "romania", "hungary", "argentina",
      "egypt", "australia", "venezuela", "saint lucia", "united states virgin islands", "moldova",
      "turks and caicos islands", "guinea", "denmark", "senegal", "syria", "bangladesh",
      "east timor",
      "djibouti", "tanzania", "qatar", "isle of man", "ireland", "tajikistan", "tristan da cunha",
      "sahrawi arab democratic republic [c]", "akrotiri and dhekelia", "christmas island",
      "sierra leone", "tuvalu", "botswana", "cayman islands", "ascension island", "guyana",
      "gibraltar",
      "burundi", "guatemala", "saint barthélemy", "guam", "bermuda", "easter island", "vietnam",
      "zimbabwe", "somaliland", "cuba", "finland", "solomon islands", "pakistan", "indonesia",
      "saint helena", "syrian opposition", "israel (de facto)  palestine (claimed)", "south sudan",
      "afghanistan", "uganda", "nepal", "sudan", "ukraine", "rwanda",
      "south georgia and the south sandwich islands", "jamaica", "norfolk island",
      "saint vincent and the grenadines", "democratic republic of the congo", "malaysia", "kuwait",
      "gabon", "malawi", "peru", "portugal", "slovenia", "togo", "united kingdom", "angola",
      "zambia",
      "luxembourg", "spain", "marshall islands", "equatorial guinea", "maldives", "nicaragua",
      "bahrain", "philippines", "mozambique", "saint martin", "lesotho", "wallis and futuna",
      "eswatini (swaziland)", "mexico", "belarus", "somalia", "monaco", "liberia", "uruguay",
      "comoros",
      "russia", "oman", "kenya", "bahamas", "myanmar", "chad", "india", "palau", "niger", "cyprus",
      "northern cyprus", "mauritania", "new caledonia", "tonga", "kazakhstan", "greenland", "aruba",
      "norway", "canada", "burkina faso", "american samoa", "federated states of micronesia",
      "panama",
      "french polynesia", "suriname", "france", "sint maarten", "cambodia", "montserrat",
      "montenegro",
      "mauritius", "papua new guinea", "vanuatu", "haiti", "trinidad and tobago", "benin",
      "czech republic", "cape verde", "south africa", "kosovo[g]", "north korea", "ecuador",
      "morocco",
      "palestine", "iceland", "latvia", "saudi arabia", "british virgin islands", "italy",
      "dominica",
      "northern mariana islands", "costa rica", "puerto rico", "san marino", "el salvador", "yemen",
      "chile", "dominican republic", "são tomé and príncipe", "bosnia and herzegovina",
      "south korea",
      "singapore", "north macedonia", "bulgaria", "sri lanka", "grenada", "jersey",
      "antigua and barbuda", "guernsey", "saint pierre and miquelon", "falkland islands", "artsakh",
      "sweden", "bolivia", "abkhazia", "fiji", "taiwan", "estonia", "kiribati", "uzbekistan",
      "georgia",
      "honduras", "iran", "bhutan", "albania", "transnistria", "japan", "faroe islands", "libya",
      "south ossetia", "tunisia", "mongolia", "liechtenstein", "malta", "anguilla", "vatican city",
      "seychelles", "austria", "laos", "lithuania", "poland", "united states", "new zealand",
      "cocos (keeling) islands", "curaçao", "namibia", "ivory coast", "cameroon", "nauru",
      "armenia",
      "croatia")


    val cities_list = city_df.select(col("Name")).collect().map(_ (0)).toSet
    val other_list1 = city_df.select(col("District")).collect().map(_ (0)).toSet
    val province = city_df.select(col("Province")).collect().map(_ (0)).toSet
    val other_list2 = other_list1 ++ province
    val other_list = other_list2 ++ countries_list

    val location = user_df.select("location")
    val check_null:String = location.select("location").as("String").collectAsList().get(0).toString

    val location_str: String = {
      //      if (location.select("location").collectAsList().isEmpty) {
      if (check_null == "[null]") {
        val nepal_str = "Nepal"
        nepal_str
      } else {
        val newDf = location.withColumn("location", regexp_replace(location("location"), """[^ \p{L} ^ \w]""", ""))
        val df = newDf.withColumn("location", regexp_replace(newDf("location"), """\s+""", " "))
        //      df.show(false)
        val loc1 = df.collectAsList().get(0)(0).toString.toLowerCase
        val loc = loc1.split(" ")

        val city_check = loc.intersect(cities_list.toList)
        val other_check = loc.intersect(other_list.toList)

        if (city_check.nonEmpty) {
          city_check(0).capitalize.toString
          //        return city_check(0)
        }
        else if (other_check.nonEmpty) {
          other_check(0).capitalize.toString
        }
        else {
          if (loc1.isEmpty) {
            val nep_str = "Nepal"
            nep_str
          } else {
            loc1.capitalize.toString
          }
        }
      }
    }
    location_str
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
//    df.show(truncate = false)
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
//      //      value.map(x=> x.map(_.toString).mkString(",")).mkString("::")
//      value.map(x => x.map(_.toString).mkString("\"", ",", "\"")).mkString(",")
//    })
//
//    val check_entities_mentions = df.select(col("entities.user_mentions").getItem(0)).collectAsList()
//    val check_entities_hashtags = df.select(col("entities.hashtags").getItem(0)).collectAsList()
//    println(check_entities_hashtags.get(0)(0))
//
//    val tweets_df = {
//      if (check_entities_mentions.get(0)(0) == null || check_entities_hashtags.get(0)(0) == null) {
//        if (check_entities_mentions.get(0)(0) == null && check_entities_hashtags.get(0)(0) != null) {
//          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//            col("entities.hashtags.text").as("hashtags")).withColumn("coordinates", explode(col("coordinates"))).
//            withColumn("coordinates", arrToString(col("coordinates")))
//        }
//        else if (check_entities_mentions.get(0)(0) != null && check_entities_hashtags.get(0)(0) == null){
//          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//            col("entities.hashtags").as("hashtags"),col("entities.user_mentions.id").as("user_mentions_id"),
//            col("entities.user_mentions.name").as("user_mentions_name")).withColumn("coordinates", explode(col("coordinates"))).
//            withColumn("coordinates", arrToString(col("coordinates")))
//        } else {
//          df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//            col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//            col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//            col("entities.hashtags").as("hashtags")).withColumn("coordinates", explode(col("coordinates"))).
//            withColumn("coordinates", arrToString(col("coordinates")))
//        }
//      }
//      else {
//        df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//          col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//          col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//          col("entities.hashtags.text").as("hashtags"), col("entities.user_mentions.id").as("user_mentions_id"),
//          col("entities.user_mentions.name").as("user_mentions_name")).withColumn("coordinates", explode(col("coordinates"))).
//          withColumn("coordinates", arrToString(col("coordinates")))
//      }
//    }
//    tweets_df.show(false)
//
//    val arrToString_Entites = udf((value: Seq[Seq[Double]]) => {
//      value.map(x => x.map(_.toString).mkString("\"", ",", "\"")).mkString(",")
//    })
//
//
//    //    val tweets_df = df.select(col("created_at"), col("id"), col("text"), col("source"), col("user.id").as("user_id"),
//    //      col("in_reply_to_status_id_str").as("in_reply_to_status_id"), col("in_reply_to_user_id_str").as("in_reply_to_user_id"),
//    //      col("lang"), col("retweet_count"), col("reply_count"), col("place.bounding_box.coordinates"),
//    //      col("entities").cast("String")).withColumn("coordinates", explode(col("coordinates"))).
//    //      withColumn("coordinates", arrToString(col("coordinates")))
//
//    val user_df = df.select("user.id", "user.name", "user.screen_name", "user.location", "user.description", "user.followers_count", "user.friends_count", "user.profile_image_url_https")
//
//    tweets_df.show(truncate = false)
//    tweets_df.printSchema()
//    //    tweets_df.printSchema()
//    //        user_df.show(truncate = true)
//
//    //    val json_tweets = tweets_df.na.fill("null").toJSON.collectAsList().get(0).toString
//    //    val json_user = user_df.na.fill("null").toJSON.collectAsList().toString
//    //    println(json_tweets)
//
//    val gp: GetProperty = new GetProperty;
//
//    tweets_df.write
//      .format("jdbc")
//      .option("url", "jdbc:postgresql://10.10.5.32:5432/twitterdb")
//      .option("dbtable", gp.getPGTweetsTable)
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

//object TestLocation {
//
//
//  def main(args: Array[String]): Unit = {
//    val read_file = new ReadJsonFile
//    val str_json = read_file.fileJson()
//    val cdf = new CreateDF
//    val location1 = cdf.json_to_df(str_json.toString: String)
//
//    val sp: CreateSparkConnection = new CreateSparkConnection;
//
//    val city_df1 = sp.spark.read.format("csv").option("header", "true").load("/Users/tchiringlama/nepali_cities.csv")
//    //    val city_df = city_df.apply(lambda x: x.astype(str).str.lower())
//    val city_df = city_df1.select(city_df1.columns.map(c => lower(col(c)).alias(c)): _*)
//
//    val countries_list = List("united arab emirates", "nigeria", "ghana", "pitcairn islands", "ethiopia", "algeria", "niue",
//      "jordan", "netherlands", "andorra", "turkey", "madagascar", "samoa", "turkmenistan",
//      "eritrea",
//      "paraguay", "greece", "cook islands", "iraq", "azerbaijan", "mali", "brunei", "thailand",
//      "central african republic", "gambia", "saint kitts and nevis", "china", "lebanon", "serbia",
//      "belize", "germany", "switzerland", "kyrgyzstan", "guinea-bissau", "colombia", "brazil",
//      "slovakia", "republic of the congo", "barbados", "belgium", "romania", "hungary", "argentina",
//      "egypt", "australia", "venezuela", "saint lucia", "united states virgin islands", "moldova",
//      "turks and caicos islands", "guinea", "denmark", "senegal", "syria", "bangladesh",
//      "east timor",
//      "djibouti", "tanzania", "qatar", "isle of man", "ireland", "tajikistan", "tristan da cunha",
//      "sahrawi arab democratic republic [c]", "akrotiri and dhekelia", "christmas island",
//      "sierra leone", "tuvalu", "botswana", "cayman islands", "ascension island", "guyana",
//      "gibraltar",
//      "burundi", "guatemala", "saint barthélemy", "guam", "bermuda", "easter island", "vietnam",
//      "zimbabwe", "somaliland", "cuba", "finland", "solomon islands", "pakistan", "indonesia",
//      "saint helena", "syrian opposition", "israel (de facto)  palestine (claimed)", "south sudan",
//      "afghanistan", "uganda", "nepal", "sudan", "ukraine", "rwanda",
//      "south georgia and the south sandwich islands", "jamaica", "norfolk island",
//      "saint vincent and the grenadines", "democratic republic of the congo", "malaysia", "kuwait",
//      "gabon", "malawi", "peru", "portugal", "slovenia", "togo", "united kingdom", "angola",
//      "zambia",
//      "luxembourg", "spain", "marshall islands", "equatorial guinea", "maldives", "nicaragua",
//      "bahrain", "philippines", "mozambique", "saint martin", "lesotho", "wallis and futuna",
//      "eswatini (swaziland)", "mexico", "belarus", "somalia", "monaco", "liberia", "uruguay",
//      "comoros",
//      "russia", "oman", "kenya", "bahamas", "myanmar", "chad", "india", "palau", "niger", "cyprus",
//      "northern cyprus", "mauritania", "new caledonia", "tonga", "kazakhstan", "greenland", "aruba",
//      "norway", "canada", "burkina faso", "american samoa", "federated states of micronesia",
//      "panama",
//      "french polynesia", "suriname", "france", "sint maarten", "cambodia", "montserrat",
//      "montenegro",
//      "mauritius", "papua new guinea", "vanuatu", "haiti", "trinidad and tobago", "benin",
//      "czech republic", "cape verde", "south africa", "kosovo[g]", "north korea", "ecuador",
//      "morocco",
//      "palestine", "iceland", "latvia", "saudi arabia", "british virgin islands", "italy",
//      "dominica",
//      "northern mariana islands", "costa rica", "puerto rico", "san marino", "el salvador", "yemen",
//      "chile", "dominican republic", "são tomé and príncipe", "bosnia and herzegovina",
//      "south korea",
//      "singapore", "north macedonia", "bulgaria", "sri lanka", "grenada", "jersey",
//      "antigua and barbuda", "guernsey", "saint pierre and miquelon", "falkland islands", "artsakh",
//      "sweden", "bolivia", "abkhazia", "fiji", "taiwan", "estonia", "kiribati", "uzbekistan",
//      "georgia",
//      "honduras", "iran", "bhutan", "albania", "transnistria", "japan", "faroe islands", "libya",
//      "south ossetia", "tunisia", "mongolia", "liechtenstein", "malta", "anguilla", "vatican city",
//      "seychelles", "austria", "laos", "lithuania", "poland", "united states", "new zealand",
//      "cocos (keeling) islands", "curaçao", "namibia", "ivory coast", "cameroon", "nauru",
//      "armenia",
//      "croatia")
//
//
//    val cities_list = city_df.select(col("Name")).collect().map(_ (0)).toSet
//    val other_list1 = city_df.select(col("District")).collect().map(_ (0)).toSet
//    val province = city_df.select(col("Province")).collect().map(_ (0)).toSet
//    val other_list2 = other_list1 ++ province
//    val other_list = other_list2 ++ countries_list
//
//    import sp.spark.implicits._
//
//    val location = location1.select("user.location")
//    val check_null:String = location.select("location").as("String").collectAsList().get(0).toString
//    println(check_null)
//
//    if (check_null == "[null]" || check_null == " ") {
//      println("NEPAL")
//    } else {
//      val newDf = location.withColumn("location", regexp_replace(location("location"), """[^ \p{L} ^ \w]""", ""))
//      val df = newDf.withColumn("location", regexp_replace(newDf("location"), """\s+""", " "))
//      //      df.show(false)
//      println("INSIDE DF")
//      df.select("location").show(false)
//
//      val loc1 = df.collectAsList().get(0)(0).toString.toLowerCase
//      val loc = loc1.split(" ")
//
//      val city_check = loc.intersect(cities_list.toList)
//      val other_check = loc.intersect(other_list.toList)
//
//      if (city_check.nonEmpty) {
//        println("CITY CHECK = " + city_check(0).capitalize)
//        println("\n")
//        //        return city_check(0)
//      }
//      else if (other_check.nonEmpty) {
//        println("OTHER_CHECK = " + other_check(0).capitalize)
//        println("\n")
//        //        return other_check(0)
//      }
//      else {
//        if (loc1.isEmpty) {
//          println("TALA KO NEPAL")
//
//        } else {
//          println("LOCATION ELSE = " + loc1.capitalize)
//        }
//      }
//    }
//  }
//}
