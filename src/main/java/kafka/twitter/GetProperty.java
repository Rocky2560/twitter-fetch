package kafka.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetProperty {
    Properties prop;

    public GetProperty() throws IOException{
        this.prop = new Properties();

        InputStream inputStream = new FileInputStream("/home/rocky/app.properties");
//        InputStream inputStream = new FileInputStream("/etc/kafka-twitter/app2.properties");
        this.prop.load(inputStream);
    }

    public String getConsumerKey(){ return this.prop.getProperty("consumerKey"); }

    public String getConsumerSecret(){ return this.prop.getProperty("consumerSecret"); }

    public String getToken(){ return this.prop.getProperty("token"); }

    public String getSecret(){ return this.prop.getProperty("secret"); }

    public String getUserTopic(){ return this.prop.getProperty("user_topic"); }

    public String getTweetsTopic(){ return this.prop.getProperty("tweet_topic"); }

    public String getBootstrapServer(){ return this.prop.getProperty("bootstrap"); }

    public String getMongoHost(){ return this.prop.getProperty("mongo_host"); }

    public String getMongoPort(){ return this.prop.getProperty("mongo_port"); }

    public String getMongoDatabase(){ return this.prop.getProperty("mongo_db"); }

    public String getMongoCollection(){ return this.prop.getProperty("mongo_collection"); }

    public String getPGUserTopic(){ return this.prop.getProperty("pg_user_topic"); }

    public String getPGTweetsTopic(){ return this.prop.getProperty("pg_tweet_topic"); }

    public String getPGUrl(){ return this.prop.getProperty("postgres_url"); }

    public String getPGUserTable(){ return this.prop.getProperty("postgres_user_table"); }

    public String getPGTweetsTable(){ return this.prop.getProperty("postgres_tweets_table"); }

    public String getPGUsername(){ return this.prop.getProperty("postgres_user"); }

    public String getPGPassword(){ return this.prop.getProperty("postgres_password"); }

    public String getCity(){ return this.prop.getProperty("city_file"); }




    //Cassandra Properties


    public String getHost() {
        return this.prop.getProperty("host");
    }

    public String getCassUser() {
        return this.prop.getProperty("user");
    }

    public String getCassPass() {
        return this.prop.getProperty("pass");
    }

    public int getPort() {
        return Integer.parseInt(this.prop.getProperty("port"));
    }

    public String getKeyspace() {
        return this.prop.getProperty("keyspace");
    }

    public String getTable_name() {
        return this.prop.getProperty("table_name");
    }

    public String getKeystore_path() {
        return this.prop.getProperty("keystore_path");
    }

    public String getKeystore_password() {
        return this.prop.getProperty("keystore_password");
    }

    public String getTruststore_path() {
        return this.prop.getProperty("truststore_path");
    }

    public String getTruststore_password() {
        return this.prop.getProperty("truststore_password");
    }

}
