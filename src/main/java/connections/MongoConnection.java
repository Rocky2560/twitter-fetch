package connections;

import com.google.gson.JsonObject;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import kafka.twitter.GetProperty;
import kafka.twitter.KakfaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.bson.Document;

import java.io.IOException;
import java.net.UnknownHostException;

public class MongoConnection {
    GetProperty gp = new GetProperty();
    KakfaProducerConfig kpc = new KakfaProducerConfig();

    public MongoConnection() throws IOException {}

    public void prodMongo(JsonObject jsonObject, KafkaProducer<String, String> producer_user, String userID) throws UnknownHostException {
        String mongo_host = "";
        int mongo_port = 0;
        MongoClient mongo = new MongoClient(mongo_host, mongo_port);
        MongoDatabase db = mongo.getDatabase("xxxx");
        MongoCollection<Document> collection = db.getCollection("xxx");

        BasicDBObject check_id = new BasicDBObject();
        check_id.put("xxx", userID);

        //query to check if id exists
        MongoCursor<Document> id_exist = collection.find(check_id).iterator();

        if (id_exist.hasNext()) {
            System.out.println("ID EXISTS");
            DBObject dbObject = BasicDBObject.parse(String.valueOf(jsonObject));
            BasicDBObject updatedDoc = new BasicDBObject();
            updatedDoc.putAll(dbObject);

            BasicDBObject searchQuery = new BasicDBObject().append("xxx", userID);
            collection.updateOne(searchQuery, updatedDoc);

        } else {
            System.out.println("DOES NOT EXIST");
            //send to kafka topic
            kpc.SendToTopic(gp.getUserTopic(), producer_user, jsonObject);
        }
    }
}


//    String mongo_host = "10.10.5.25";
//    int mongo_port = 27017;
//    MongoClient mongo = new MongoClient(mongo_host, mongo_port);
//    DB db = mongo.getDB("twitterdb");
//    DBCollection col = db.getCollection("userinfo");
//
//
//    //query if user exists
////        DBObject query = BasicDBObjectBuilder.start().add("id_str", "3681732554").get();
//    DBObject query = BasicDBObjectBuilder.start().get();
//    DBCursor cursor = col.find(query);
//        while (cursor.hasNext()){
//                System.out.println(cursor.next());
//                }
//                DBObject updateDoc = new BasicDBObject();
//
//                col.update(query, updateDoc);
