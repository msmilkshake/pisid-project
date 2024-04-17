package com.tailwaglabs.core.migrator;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class ConnectionFactory {

    public static MongoCollection<Document> getNewConnection(String host, int port, String database, String collection) {

        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase db = mongoClient.getDatabase(database);
        return db.getCollection(collection);
    }
}
