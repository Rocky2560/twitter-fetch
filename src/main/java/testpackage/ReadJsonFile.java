package testpackage;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileReader;
import java.io.IOException;

public class ReadJsonFile {
    public JsonObject fileJson() throws IOException {
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonObject = jsonParser.parse(new FileReader("/Users/tchiringlama/tweet1.json"));
        return (JsonObject) jsonObject;
    }
}
