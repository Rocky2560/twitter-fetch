//package com.kafka.twitter.prod;
//
//import com.google.gson.JsonParser;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.KStream;
//
//import java.util.Properties;
//
//public class StreamFilterTweets {
//
//    //create properties
//    public static void main(String[] args) {
////    public void Filter(){
//        String bootstrap = "10.10.5.32:9092";
//        String topic = "test-tweets";
//
//        Properties properties = new Properties();
//        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
//        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo");
//        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
//        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
//
//        StreamsBuilder streamsBuilder = new StreamsBuilder();
//
//        KStream<String, String> inputTopic = streamsBuilder.stream(topic);
//        KStream<String,String> filteredStream = inputTopic.filter(
//                (k, jsonTweet) -> extractUserFromTweet(jsonTweet).equals("NP")
//        );
//        System.out.println(filteredStream);
//        filteredStream.to("nepal-tweets");
//
//        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
//        kafkaStreams.start();
//    }
//
//    private static JsonParser jsonParser = new JsonParser();
//
//    private static String extractUserFromTweet(String tweetJson){
//        try {
//            return jsonParser.parse(tweetJson)
//                    .getAsJsonObject()
//                    .get("place")
//                    .getAsJsonObject()
//                    .get("country_code")
//                    .getAsString();
//        }
//        catch (NullPointerException e){
//            return String.valueOf(e);
////            return 0;
//        }
//    }
//
//}
