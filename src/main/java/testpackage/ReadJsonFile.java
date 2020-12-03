package testpackage;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.json.JSONArray;
import org.json4s.jackson.Json;

import java.io.*;
import java.util.*;

public class ReadJsonFile {
    public JsonObject fileJson() throws IOException {
        JsonParser jsonParser = new JsonParser();
//        JsonElement jsonObject = jsonParser.parse(new FileReader("/Users/tchiringlama/tweets4"));
//        JsonElement jsonObject = jsonParser.parse(new FileReader("/Users/tchiringlama/tweet2"));
//        JsonElement jsonObject = jsonParser.parse(new FileReader("/home/tchiring/test-twitter"));
        String content = new Scanner((new File("/home/tchiring/test-twitter"))).next();
        List<String> jo = Collections.singletonList(content);

        System.out.println(jo.get(0));


        return null;


//        return (JsonObject) jsonObject;
    }


    public void ReadFile() throws IOException {
        FileReader fr = new FileReader("/home/tchiring/twitter-test");
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while(line != null){
            System.out.println("\n" + line);
            line = br.readLine();
        }
    }

    public static void main(String[] args) throws IOException {
        ReadJsonFile rj = new ReadJsonFile();
        rj.fileJson();
//        rj.ReadFile();

    }
}
