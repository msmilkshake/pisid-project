package com.tailwaglabs.core.migrator;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * Migrates and processes Movs sensor data: Mongo - MySQL
 * Executes - When an experiment is started (Django button starts this)
 */
public class MovsMigrator {

    private MongoClient mongoClient;
    private MongoDatabase db;
    private String host = "localhost";
    private int port = 27019;
    private String database = "mqqt";
    private String collection = "movs";
    private MongoCollection<Document> movsCollection;
    private long currentTimestamp;
    private MongoCursor<Document> cursor;

    Document movsQuery = Document.parse("{q: [" +
            "{ $match: {  $and: [ { Timestamp: { $gte: " + currentTimestamp + " } }, {Migrated: {$ne \"1\" }} ] } }," +
            "{ $project: {_id: 1, Hora: 1, SalaDestino: 1, SalaOrigem: 1, Hora: 1 } }" +
            "]}"
    );

    private void init() {
        mongoClient = new MongoClient(host, port);
        db = mongoClient.getDatabase(database);
        movsCollection = db.getCollection(collection);
        currentTimestamp = System.currentTimeMillis();
        cursor = movsCollection.find(movsQuery).iterator();
    }

    private void closeConnection() {
        cursor.close();
        mongoClient.close();
    }

    private void loop() {
        init();
        while(cursor.hasNext()) {
            Document mongoRecord = cursor.next();
        }
        closeConnection();
    }

    public void checkWrongTimestamp(Document doc) {
        // TODO
    }

    public void checkWrongFormat() {
        // TODO
    }

    public void insertRecordMysql(Document doc) {
        // TODO
    }




    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Movs_Migration");

        new MovsMigrator().loop();
        
    }
    
}
