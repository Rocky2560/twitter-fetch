package kafka.twitter;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import connections.MongoConnection;
import connections.PostgresConnection;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.ExplodeInsert;


import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    GetProperty gp = new GetProperty();
    String consumerKey;
    String consumerSecret;
    String token;
    String secret;
    String bootstrapServers;
    JsonParser jsonParser = new JsonParser();
    JsonObject userinfo;
    JsonObject tweetinfo;

    KakfaProducerConfig kpc = new KakfaProducerConfig();
    MongoConnection mc = new MongoConnection();

    public TwitterProducer() throws IOException {
        this.consumerKey = gp.getConsumerKey();
        this.consumerSecret = gp.getConsumerSecret();
        this.token = gp.getToken();
        this.secret = gp.getSecret();
        this.bootstrapServers = gp.getBootstrapServer();
    }

    public static void main(String[] args) throws IOException, SQLException {
        new TwitterProducer().run();
    }

    public void run() throws IOException, SQLException {
        logger.info("Setup");
        //create a twitter client

        BlockingQueue<String> msgQueue = new LinkedBlockingDeque<String>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer_user = kpc.createKafkaProducer();
        KafkaProducer<String, String> producer_tweets = kpc.createKafkaProducer();

        PostgresConnection pgc = new PostgresConnection();

        KafkaProducer<String, String> pg_producer_user = kpc.createKafkaProducer();
        KafkaProducer<String, String> pg_producer_tweets = kpc.createKafkaProducer();

        ExplodeInsert expInsert = new ExplodeInsert();


        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
//                System.out.println(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                String country_code = jsonParser.parse(msg)
                        .getAsJsonObject()
                        .get("place")
                        .getAsJsonObject()
                        .get("country_code")
                        .getAsString();

                if (country_code.equals("NP"))
                {
                    userinfo = getUserObject(msg);
//                    Check if Data Exists in MongoDB
                    mc.prodMongo(userinfo, producer_user, getUserID(userinfo), kpc);

                    //Check if Data Exists in Postgres
                    pgc.checkExist(gp.getPGUserTopic(), kpc, pg_producer_user, msg);

                    //Send Tweets Data to:
                    tweetinfo = getTweetObject(msg, userinfo);
                    //MongoDB
                    kpc.SendToTopic(gp.getTweetsTopic(), producer_tweets, tweetinfo);
                    //Postgres
                    expInsert.InsertTweets(msg);
//                    kpc.SendToTopic(gp.getPGTweetsTopic(), pg_producer_tweets, (JsonObject) jsonParser.parse(expInsert.tweetsInfo(msg)));
                }
            }
        }
        logger.info("End of application");
//        producer_tweets.close();
    }

    private String getUserID(JsonObject userinfo){
        String user_id = userinfo
                .get("id_str")
                .getAsString();
        return user_id;
    }

    private JsonObject getUserObject(String msg) {
        JsonObject user_info;
        try {
            user_info = jsonParser.parse(msg)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject();

        }catch (NullPointerException e){
            logger.error("No field user in json ", e);
            return null;
        }
        return user_info;
    }

    private JsonObject getTweetObject(String msg,JsonObject userinfo){
        JsonObject tweet_info;

        String screen_name;
        String user_id;

        try {
            tweet_info = jsonParser.parse(msg)
                    .getAsJsonObject();

            user_id = userinfo
                    .get("id_str")
                    .getAsString();

            screen_name = userinfo
                    .get("screen_name")
                    .getAsString();

            tweet_info.addProperty("user_id", user_id);
            tweet_info.addProperty("screen_name", screen_name);

            tweet_info.remove("user");

        } catch (NullPointerException e){
            logger.error("Empty data set ", e);
            return null;
        }
        return tweet_info;
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms

//        List<String> terms = Lists.newArrayList("YetiAirlines");
//        hosebirdEndpoint.trackTerms(terms);

//        List<Long> id = Lists.newArrayList(25073877L);
//        hosebirdEndpoint.followings(id);

        hosebirdEndpoint.locations(Arrays.asList(

                //Kathmandu and Pokhara
//                new Location(
//                        new Location.Coordinate(84.8145316838, 27.2710335212), // south west
//                        new Location.Coordinate( 85.7192215389,    28.1488221902)),
//                new Location(
//                        new Location.Coordinate(83.5464124476, 27.732952078), // south west
//                        new Location.Coordinate( 84.4351982851,    28.5943287285))
//
                                new Location(
                        new Location.Coordinate(79.8695023432, 25.6533705916), // south west
                        new Location.Coordinate( 88.4373289467,    30.6875340549))
        ));

        // These secrets are read from config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.
//        hosebirdClient.connect();
    }
}
