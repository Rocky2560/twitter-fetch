//package testpackage;
//
//import com.google.common.collect.Lists;
//import com.twitter.hbc.ClientBuilder;
//import com.twitter.hbc.core.Client;
//import com.twitter.hbc.core.Constants;
//import com.twitter.hbc.core.Hosts;
//import com.twitter.hbc.core.HttpHosts;
//import com.twitter.hbc.core.endpoint.Location;
//import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
//import com.twitter.hbc.core.processor.StringDelimitedProcessor;
//import com.twitter.hbc.httpclient.auth.Authentication;
//import com.twitter.hbc.httpclient.auth.OAuth1;
//import kafka.twitter.GetProperty;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import spark.CassandraInserts;
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.sql.SQLException;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingDeque;
//import java.util.concurrent.TimeUnit;
//
//public class FetchTweets {
//
//
//    Logger logger = LoggerFactory.getLogger(kafka.twitter.TwitterProducer.class.getName());
//
//    GetProperty gp = new GetProperty();
//    String consumerKey;
//    String consumerSecret;
//    String token;
//    String secret;
//
//    public FetchTweets() throws IOException {
//        this.consumerKey = gp.getConsumerKey();
//        this.consumerSecret = gp.getConsumerSecret();
//        this.token = gp.getToken();
//        this.secret = gp.getSecret();
//    }
//
//    public static void main(String[] args) throws IOException, SQLException {
//        new FetchTweets().run();
//    }
//
//    public void run() throws IOException {
//        logger.info("Setup");
//        //create a twitter client
//        BlockingQueue<String> msgQueue = new LinkedBlockingDeque<String>(1000);
//
//        Client client = createTwitterClient(msgQueue);
//        client.connect();
//
//        //loop to send tweets to kafka
//        // on a different thread, or multiple different threads....
//        FileWriter fw = new FileWriter("/home/tchiring/twitter-test.txt");
//        BufferedWriter bw = new BufferedWriter(fw);
//        while (!client.isDone()) {
//            String msg = null;
//            try {
//                msg = msgQueue.poll(5, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//                client.stop();
//            }
//            if (msg != null) {
//                try {
//                    System.out.println(msg);
//                    bw.append(msg).append("\n");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        logger.info("End of application");
////        producer_tweets.close();
//    }
//
//
//    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
//
//        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
//        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
//        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
//// Optional: set up some followings and track terms
//
//        List<String> terms = Lists.newArrayList("#YetiAirlines", "#yetiairlines", "FlyYeti",
//                "#Foodmandu", "Foodmandu", "#foodmandu", "foodmandu",
////                "#BigMart", "#BIGMART", "#Bigmart", "bigmart", "Bigmart",
//                "Bigmart Nepal", "bigmart Nepal", "$BigMart",
//                "Bhatbhateni", "#Bhatbhateni", "bhatbhateni", "#bhatbhateni",
//                "onlinekhabar", "Online_khabar", "#onlinekhabar", "#Onlinekhabar", "#OnlineKhabar",
//                "#hamropatro", "#Hamropatro", "#HamroPatro", "hamropatro", "HamroPatro", "Hamro Patro",
//                "#valleycoldstore", "Valley Cold Store", "#valleyColdStore", "#Valleycoldstore",
//                "#orlando", "#Orlando",
//                "BhojDeals", "bhojdeals", "#bhojdeals",
//                "Foodmario", "globalfoodmario", "#foodmario", "#Foodmario",
//                "Tatomitho", "tatomitho",
//                "ekantinpur", "Ekantipur", "ekantipur_com",
//                "Annapurna Post", "annapurnapost", "annapurna post",
//                "nagarik_news", "nagariknews",
//                "12khari", "baahrakhari",
//                "CitizenPost News", "Cpostnews", "#cpostnews",
//                "hamrakura",
//                "Saleways", "saleways",
//                "Salesberry", "#salesberry", "#Salesberry", "salesberry",
//                "atsumari_llc",
//                "laguerison_jp"
//        );
//
//        hosebirdEndpoint.trackTerms(terms);
//
//        List<Long> id = Lists.newArrayList(214414640L, 1533033576L, 574780737L, 365918709L, 3832931652L, 2562624290L,
//                2713545668L,
//                219041453L,          //@OrlandoPolice
//                564668361L,          //@OrangeCoFL
//                20173932L,          //@OCCC
//                798973032362606600L, //@RepValDemings
//                818713465653051392L, //@RepDarrenSoto
//                30924790L,            //@orlandomayor
//                3145814942L,            //BhojDeals
//                875680481651695616L,       //foodmario
//                1233344167385780224L,       //tatomitho
//                364761398L,                 //ekantipur_com
//                1568130360L,                //Annapurna_Post
//                2308483814L,                //nagarik_news
//                4886144923L,                    //12khari
//                859986491879051264L,          //Cpostnews
//                1001701014L,                   //hamrakura
//                1090509140923944960L,              //SalesBerry2
//                1280391401058914304L,            //atsumari_llc
//                1096213091405115393L                //laguerison_jp
//        );
//        hosebirdEndpoint.followings(id);
//
////        hosebirdEndpoint.locations(Arrays.asList(
////
////                //Kathmandu and Pokhara
//////                new Location(
//////                        new Location.Coordinate(84.8145316838, 27.2710335212), // south west
//////                        new Location.Coordinate( 85.7192215389,    28.1488221902)),
//////                new Location(
//////                        new Location.Coordinate(83.5464124476, 27.732952078), // south west
//////                        new Location.Coordinate( 84.4351982851,    28.5943287285))
//////
////                new Location(
////                        new Location.Coordinate(79.8695023432, 25.6533705916), // south west
////                        new Location.Coordinate(88.4373289467, 30.6875340549))
////        ));
//
//        // These secrets are read from config file
//        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
//
//
//        ClientBuilder builder = new ClientBuilder()
//                .name("Hosebird-Client-01")                              // optional: mainly for the logs
//                .hosts(hosebirdHosts)
//                .authentication(hosebirdAuth)
//                .endpoint(hosebirdEndpoint)
//                .processor(new StringDelimitedProcessor(msgQueue));
////                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events
//
//        Client hosebirdClient = builder.build();
//        return hosebirdClient;
//// Attempts to establish a connection.
////        hosebirdClient.connect();
//    }
//}
//
