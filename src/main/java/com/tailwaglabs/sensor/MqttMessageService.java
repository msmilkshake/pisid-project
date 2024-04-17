package com.tailwaglabs.sensor;

public class MqttMessageService {

    // Handles all mqtt messages from the Cloud to Mongo
    // Executes - ALWAYS 24/7 (manually)
    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Mqtt_Subscriber");
        
    }
}
