package com.tailwaglabs.core.migrator;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.Connection;
import java.sql.DriverManager;

// This Class isn't in use, the code was replicated in the other classes
public class ConnectionFactory {

    public static MongoCollection<Document> getNewConnection(String host, int port, String database, String collection) {

        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase db = mongoClient.getDatabase(database);
        return db.getCollection(collection);
    }


}
