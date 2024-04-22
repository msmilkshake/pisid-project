package com.tailwaglabs.core;

public class Sensor {
    private int id;
    private String time;
    private double reading;
    private int sensorNumber;
    private long timestamp;

    public Sensor(int id, String time, double reading, long timestamp) {
        this.id = id;
        this.time = time;
        this.reading = reading;
        this.sensorNumber = sensorNumber;
        this.timestamp = timestamp;
    }
}
