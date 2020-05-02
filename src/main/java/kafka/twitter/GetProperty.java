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

    public int getMongoPort(){ return Integer.parseInt(this.prop.getProperty("mongo_port")); }

    public String getMongoDatabase(){ return this.prop.getProperty("mongo_db"); }

    public String getMongoCollection(){ return this.prop.getProperty("mongo_collection"); }


}