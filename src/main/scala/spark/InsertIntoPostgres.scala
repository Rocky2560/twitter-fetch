//package spark
//
//import java.util.Properties
//
//class InsertIntoPostgres {
//  val sp: CreateSparkConnection = new CreateSparkConnection
//
////  val jdbcDF = sp.spark.read
////    .format("jdbc")
////    .option("url", "jdbc:postgresql:dbserver")
////    .option("dbtable", "schema.tablename")
////    .option("user", "username")
////    .option("password", "password")
////    .load()
//
//  val connectionProperties = new Properties()
//  connectionProperties.put("user", "twitter")
//  connectionProperties.put("password", "twitter123")
//
////  val jdbcDF2 = sp.spark.read
////    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
//////   Specifying the custom data types of the read schema
////  connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
//
////  val jdbcDF3 = sp.spark.read
////    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
//
//  // Saving data to a JDBC source
//  df.write
//    .format("jdbc")
//    .option("url", "jdbc:postgresql:dbserver")
//    .option("dbtable", "public.tweetsinfo")
//    .option("user", "twitter")
//    .option("password", "twitter123")
//    .save()
//
//  jdbcDF2.write
//    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
//
//  // Specifying create table column data types on write
//  jdbcDF.write
//    .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
//    .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
//
//}
