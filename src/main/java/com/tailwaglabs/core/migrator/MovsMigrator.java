package com.tailwaglabs.core.migrator;

import com.mongodb.Mongo;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

/**
 * Migrates and processes Movs sensor data: Mongo - MySQL
 * Executes - When an experiment is started (Django button starts this)
 */
public class MovsMigrator {

    private String host = "localhost";
    private int port = 27019;
    private String database = "mqqt";
    private String collection = "movs";

    private MongoCollection<Document> movsCollection;
    private long currentTimestamp;
    private MongoCursor<Document> cursor;

    Document movsQuery = Document.parse("{q: [" +
            "{ $match: { Timestamp: { $gte: " + currentTimestamp + " } } }," +
            "{ $project: {_id: 1, Timestamp: 1, SalaDestino: 1, SalaOrigem: 1, Hora: 1 } }" +
            "]}"
    );

    private void init() {
        movsCollection = ConnectionFactory.getNewConnection(host, port, database, collection);
        currentTimestamp = System.currentTimeMillis();
    }

    private void loop() {
        init();
    }




    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Movs_Migration");

        new MovsMigrator().loop();
        
    }
    
}
