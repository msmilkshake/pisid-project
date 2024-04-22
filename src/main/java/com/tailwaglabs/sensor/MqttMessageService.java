package com.tailwaglabs.sensor;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import javax.swing.*;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;

public class MqttMessageService {

    MqttClient mqttclient;
    static MongoClient mongoClient;
    static DB db;
    static DBCollection mongocol;
    static String mongo_user = new String();
    static String mongo_password = new String();
    static String mongoHost = new String();
    static String cloud_server = new String();
    static String cloud_temp_topic = new String();
    static String cloud_mov_topic = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
    static String mongo_database = new String();
    static String mongo_temp_collection = new String();
    static String mongo_mov_collection = new String();
    static String mongo_authentication = new String();


    public void start() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("config.ini"));
            mongoHost = p.getProperty("localhost");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            cloud_server = p.getProperty("cloud_server");
            cloud_temp_topic = p.getProperty("cloud_temp_topic");
            cloud_mov_topic = p.getProperty("cloud_mov_topic");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_temp_collection = p.getProperty("mongo_temp_collection");
            mongo_mov_collection = p.getProperty("mongo_mov_collection");
        } catch (Exception e) {
            System.out.println("Error reading CloudToMongo.ini file " + e);
            JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo", JOptionPane.ERROR_MESSAGE);
        }
        new CloudToMongo().connecCloud();
        new CloudToMongo().connectMongo();
    }

    private void connectCloud() {
        int i;
        try {
            i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "CloudToMongo_"+String.valueOf(i)+"_"+"g14");
            mqttclient.connect();
            System.out.println("Connected with success:" + mqttclient.isConnected());
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_temp_topic);
            mqttclient.subscribe(cloud_mov_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void connectMongo() {
        String mongoURI = new String();
        mongoURI = "mongodb://";
        if (mongo_authentication.equals("true")) mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongoHost;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?replicaSet=" + mongo_replica+"&authSource=admin";
            else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else
        if (mongo_authentication.equals("true")) mongoURI = mongoURI  + "/?authSource=admin";
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        db = mongoClient.getDB(mongo_database);
    }

    // Handles all mqtt messages from the Cloud to Mongo
    // Executes - ALWAYS 24/7 (manually)
    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Mqtt_Subscriber");
        
    }
}
