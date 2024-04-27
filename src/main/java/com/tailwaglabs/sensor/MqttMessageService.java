package com.tailwaglabs.sensor;

import com.mongodb.*;
import com.mongodb.util.JSON;
import com.tailwaglabs.core.Logger;
import org.eclipse.paho.client.mqttv3.*;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;

public class MqttMessageService implements MqttCallback {

    MqttClient mqttclient;

    static DB db;
    static DBCollection mongocol;
    static String mongo_user = new String();
    static String mongo_password = new String();
    static String mongo_address = new String();
    static String cloud_server = new String();
    static String cloud_temp_topic = new String();
    static String cloud_mov_topic = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
    static String mongo_database = new String();
    static String mongo_temp_collection = new String();
    static String mongo_mov_collection = new String();
    static String mongo_authentication = new String();

    public static final String CLIENT_ID = "CloudToMongo_" + new Random().nextInt(100000) + "_" + "g14";

    static Logger logger = null;

    public static void main(String[] args) {
        Thread.currentThread().setName("MQTT_Message_Service");
        logger = new Logger(Thread.currentThread().getName());
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("config.ini"));
            mongo_address = p.getProperty("mongo_address");
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
        }
        new MqttMessageService().connecCloud();
        new MqttMessageService().connectMongo();
    }

    public void connecCloud() {
        try {
            mqttclient = new MqttClient(cloud_server, CLIENT_ID);
            mqttclient.connect();
            System.out.println("Connected with success:" + mqttclient.isConnected());
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_temp_topic);
            mqttclient.subscribe(cloud_mov_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void connectMongo() {
        String mongoURI = "mongodb://";
        if (mongo_authentication.equals("true")) mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true"))
                mongoURI = mongoURI + "/?replicaSet=" + mongo_replica + "&authSource=admin";
            else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?authSource=admin";

        db = new MongoClient(new MongoClientURI(mongoURI)).getDB(mongo_database);
    }

    @Override
    public void messageArrived(String topic, MqttMessage incomingMessage) throws Exception {
        try {

            DBObject document_json;
            document_json = (DBObject) JSON.parse(incomingMessage.toString());
            if (topic.equals(cloud_temp_topic)) {
                mongocol = db.getCollection(mongo_temp_collection);
            } else if (topic.equals(cloud_mov_topic)) {
                mongocol = db.getCollection(mongo_mov_collection);
            }
            document_json.put("Timestamp", System.currentTimeMillis());
            mongocol.insert(document_json);
            logger.log(incomingMessage);

        } catch (Exception e) {
            logger.log(e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}