package kafka.twitter.prop;

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




}
