package kafka.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetProperty {
    Properties prop;

    public GetProperty() throws IOException{
        this.prop = new Properties();

        InputStream inputStream = new FileInputStream("/etc/kafka-twitter/app.properties");
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

    public String getPGUserTopic(){ return this.prop.getProperty("pg_tweet_topic"); }

    public String getPGTweetsTopic(){ return this.prop.getProperty("pg_tweet_topic"); }

    public String getPGUrl(){ return this.prop.getProperty("postgres_url"); }

    public String getPGUserTable(){ return this.prop.getProperty("postgres_user_table"); }

    public String getPGTweetsTable(){ return this.prop.getProperty("postgres_tweets_table"); }

    public String getPGUsername(){ return this.prop.getProperty("postgres_user"); }

    public String getPGPassword(){ return this.prop.getProperty("postgres_password"); }

    public String getCity(){ return this.prop.getProperty("city_file"); }



}
