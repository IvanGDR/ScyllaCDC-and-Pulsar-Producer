package com.example;

//import java.util.concurrent.CompletableFuture;
import org.json.JSONObject;

//import io.netty.util.concurrent.CompleteFuture;



public class JsonConvert {

    public String getSensorIdValue(String message) {
        //String jsonString = message;
        JSONObject jsonObject = new JSONObject(message);
        String sensor_id = jsonObject.getString("sensor_id");
        //System.out.print(sensor_id);
        return sensor_id;
    }

    public int getCoordinateValue(String message) {
        //String jsonString = message;
        JSONObject jsonObject = new JSONObject(message);
        int coordinate = jsonObject.getInt("coordinate");
        //System.out.print(coordinate);
        return coordinate;
    }

    public String getStatusValue(String message) {
        //String jsonString = message;
        JSONObject jsonObject = new JSONObject(message);
        String status = jsonObject.getString("status");
        //System.out.print(status);
        return status;
    }
}
