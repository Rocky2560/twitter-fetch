package connections;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import kafka.twitter.GetProperty;
import kafka.twitter.KakfaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.bson.Document;
import testpackage.ReadJsonFile;

import java.io.IOException;
import java.net.UnknownHostException;
import static com.mongodb.client.model.Filters.eq;

public class MongoConnection {


    GetProperty gp = new GetProperty();

    String mongo_host = gp.getMongoHost();
    int mongo_port = gp.getMongoPort();
    String mongo_db = gp.getMongoDatabase();
    String mongo_collection = gp.getMongoCollection();

    public MongoConnection() throws IOException {}

    public void prodMongo(JsonObject jsonObject,
                          KafkaProducer<String, String> producer_user,
                          String userID,
                          KakfaProducerConfig kpc) {

        MongoClient mongo = new MongoClient(mongo_host, mongo_port);
        MongoDatabase db = mongo.getDatabase(mongo_db);
        MongoCollection<Document> collection = db.getCollection(mongo_collection);

        BasicDBObject check_id = new BasicDBObject();
        check_id.put("id_str", userID);

        //query to check if id exists
        MongoCursor<Document> id_exist = collection.find(check_id).iterator();

        if (id_exist.hasNext()) {
            collection.replaceOne(eq("id_str", userID), Document.parse(jsonObject.toString()));

        } else {
            //send to kafka topic
            kpc.SendToTopic(gp.getUserTopic(), producer_user, jsonObject);
        }
    }

//    public static void main(String[] args) throws IOException {
//        MongoConnection mc = new MongoConnection();
//        ReadJsonFile rjf = new ReadJsonFile();
//
//        KakfaProducerConfig kpc = new KakfaProducerConfig();
//        KafkaProducer<String, String> producer_user = kpc.createKafkaProducer();
//
//        mc.prodMongo(rjf.fileJson(), producer_user, mc.getUserID(rjf.fileJson()), kpc);
//    }

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
