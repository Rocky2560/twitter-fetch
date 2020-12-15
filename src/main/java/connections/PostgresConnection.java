package connections;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.twitter.GetProperty;
//import kafka.twitter.KakfaProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import spark.ExplodeInsert;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class PostgresConnection {
    private final Connection conn;
//    KakfaProducerConfig kpc = new KakfaProducerConfig();
    GetProperty gp = new GetProperty();
    ExplodeInsert explode = new ExplodeInsert();
//    ReadJsonFile rjf = new ReadJsonFile();
    JsonParser jsonParser = new JsonParser();

    String url = "";
    String user = "";
    String password = "";
    String user_table = "";

    public PostgresConnection() throws SQLException, IOException {
        this.url = gp.getPGUrl();
        this.user = gp.getPGUsername();
        this.password = gp.getPGPassword();
        this.user_table = gp.getPGUserTable();
        this.conn = getConn();
    }

    public Connection getConn() throws SQLException {
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }


    //Checks if user exists or not and performs actions based on it.
//    public void checkExist(String topic, KakfaProducerConfig kpc, KafkaProducer<String,String> user_producer, String msg, String check_country) throws SQLException, IOException {
    public void checkExist(String msg, String check_country) throws SQLException, IOException {

        String check_id_query = "select id from "+user_table+" where id = "+ getUserID(msg) +"";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(check_id_query);
        if (rs.next()){
            //update the user info
            updateInfo(getUserID(msg), msg);

        } else {
            //send to postgres
            if (check_country.equals("y")){
                explode.InsertUserInfo(msg);
            } else if (check_country.equals("n")){
                explode.InsertSpecificUserInfo(msg);
            }

            //send to kafka
//            kpc.SendToTopic("test-tweets", user_producer, (JsonObject) jsonParser.parse(explode.convertStr(msg)));
//            kpc.SendToTopic(topic, user_producer, (JsonObject) jsonParser.parse(explode.convertStr(msg)));
        }
    }

    private BigInteger getUserID(String msg) throws IOException {
//        String str = expInsert.userInfo(rjf.fileJson().toString());
        String str = explode.convertStr(msg);

        BigInteger user_id = jsonParser.parse(str).getAsJsonObject().get("id").getAsBigInteger();
        return user_id;
    }

    //If user already exists updates User information
    public void updateInfo(BigInteger user_id, String msg) throws SQLException, IOException {

//        String str = expInsert.userInfo(rjf.fileJson().toString());
        String str = explode.convertStr(msg);
        List<String> columns = Arrays.asList("name", "screen_name", "description", "location", "followers_count", "friends_count", "profile_image_url_https");

        PreparedStatement statement = conn.prepareStatement("update "+user_table+" "
        + "SET name = ?, screen_name = ?, description = ?, location = ?, followers_count = ?, friends_count = ?, profile_image_url_https = ? where id = "+user_id+"");

        for (int i = 0; i <7; i++){
            if (i == 4 || i ==5 ){
                statement.setInt(i+1, jsonParser.parse(str).getAsJsonObject().get(columns.get(i)).getAsInt());
            }
            else {
                statement.setString(i+1, jsonParser.parse(str).getAsJsonObject().get(columns.get(i)).getAsString());
            }
        }
        statement.executeUpdate();
    }

}
