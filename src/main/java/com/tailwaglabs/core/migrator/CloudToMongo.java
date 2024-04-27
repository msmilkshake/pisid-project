package com.tailwaglabs.core.migrator;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.eclipse.paho.client.mqttv3.*;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;

public class CloudToMongo implements MqttCallback {
    MqttClient mqttclient;
    static MongoClient mongoClient;
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
    static JTextArea documentLabel = new JTextArea("\n");


    private static void createWindow() {
        JFrame frame = new JFrame("Cloud to Mongo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JLabel textLabel = new JLabel("Data from broker: ",SwingConstants.CENTER);
        textLabel.setPreferredSize(new Dimension(600, 30));
        JScrollPane scroll = new JScrollPane (documentLabel, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        JButton b1 = new JButton("Stop the program");
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
        frame.setLocationRelativeTo(null);
        frame.pack();
        frame.setVisible(true);
        b1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                System.exit(0);
            }
        });
    }

    public static void main(String[] args) {
        createWindow();
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("cloudToMongo.ini"));
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
            JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo", JOptionPane.ERROR_MESSAGE);
        }
        new CloudToMongo().connecCloud();
        new CloudToMongo().connectMongo();
    }

    public void connecCloud() {
        int i;
        try {
            i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "CloudToMongo_"+String.valueOf(i)+"_"+"g14");
            mqttclient.connect();
            System.out.println("Connected with success:" + mqttclient.isConnected());
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_temp_topic); // REINSTATE
            mqttclient.subscribe(cloud_mov_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void connectMongo() {
        String mongoURI = new String();
        mongoURI = "mongodb://";
        if (mongo_authentication.equals("true")) mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?replicaSet=" + mongo_replica+"&authSource=admin";
            else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else
        if (mongo_authentication.equals("true")) mongoURI = mongoURI  + "/?authSource=admin";
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        db = mongoClient.getDB(mongo_database);
        // mongocol = db.getCollection(mongo_collection);
    }

    @Override
    public void messageArrived(String topic, MqttMessage c)
            throws Exception {
        try {
            
            DBObject document_json;
            document_json = (DBObject) JSON.parse(c.toString());
            if (topic.equals(cloud_temp_topic)) {
                mongocol = db.getCollection(mongo_temp_collection);
            } else if (topic.equals(cloud_mov_topic)) {
                mongocol = db.getCollection(mongo_mov_collection);
            }
            document_json.put("Timestamp", System.currentTimeMillis());
            mongocol.insert(document_json);
            documentLabel.append(c.toString() + "\n");
            documentLabel.setCaretPosition(documentLabel.getDocument().getLength());
            
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}