package com.example;


public class PulsarJsonSchema {

    String sensor_id;
    int coordinate;
    String status;


    // Getters and setters
    public String getSensorID() {
        return sensor_id;
    }

    public void setSensorID(String sensor_id) {
        this.sensor_id = sensor_id;
    }

    public int getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(int coordinate) {
        this.coordinate = coordinate;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
   
}
