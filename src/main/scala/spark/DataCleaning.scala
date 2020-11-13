package spark

import kafka.twitter.GetProperty
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower, regexp_replace}

class DataCleaning {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sp: CreateSparkConnection = new CreateSparkConnection;
  val gp: GetProperty = new GetProperty();

  def LocationCleaning(user_df: DataFrame): String = {
    //    val city_df1 = sp.spark.read.format("csv").option("header", "true").load("/Users/tchiringlama/nepali_cities.csv")
    val city_df1 = sp.spark.read.format("csv").option("header", "true").load(gp.getCity)
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
    val check_null: String = location.select("location").as("String").collectAsList().get(0).toString

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
