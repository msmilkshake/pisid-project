package com.tailwaglabs.core.migrator;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.ExperimentWatcher;
import org.bson.Document;

/**
 * Migrates and processes Temps sensor data: Mongo - MySQL
 * Executes - Same time as the Django app (Can be a script that runs both)
 *
 * This thread only processes some data if an experiment is running
 * If no experiment is running only processes: Outliers and Incorrect data.
 */
public class TempsMigrator {

    private MongoClient mongoClient;
    private MongoDatabase db;
    private String host = "localhost";
    private int port = 27019;
    private String database = "mqqt";
    private String collection = "temps";
    private MongoCollection<Document> tempsCollection;
    private long currentTimestamp;
    private MongoCursor<Document> cursor;
    private boolean experimentRunning;

    Document movsQuery = Document.parse("{q: [" +
            "{ $match: {  $and: [ { Timestamp: { $gte: " + currentTimestamp + " } }, {Migrated: {$ne \"1\" }} ] } }," +
            "{ $project: {_id: 1, Hora: 1, Leitura: 1, Sensor: 1 } }" +
            "]}"
    );

    private void init() {
        mongoClient = new MongoClient(host, port);
        db = mongoClient.getDatabase(database);
        tempsCollection = db.getCollection(collection);
        currentTimestamp = System.currentTimeMillis();
        cursor = tempsCollection.find(movsQuery).iterator();
    }

    private void closeConnection() {
        cursor.close();
        mongoClient.close();
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

    public void checkLimitProximity(Document doc) {
        double actualTemp = (double) doc.get("Leitura");

        //Tem de se ir buscar à experiencia
        double tempMin = 0;
        double tempMax = 0;

        double range = tempMax - tempMin;

        double minLimit = tempMin + 0.1 * range;
        double maxLimit = tempMin + 0.9 * range;

        if (actualTemp <= minLimit) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura próxima do limite inferior");
        } else if (actualTemp >= maxLimit) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura próxima do limite superior");
        }
    }

    public void limitReached(Document doc) {
        double actualTemp = (double) doc.get("Leitura");

        //Tem de se ir buscar à experiencia
        double tempMin = 0;
        double tempMax = 0;

        if (actualTemp <= tempMin) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura atingiu limite inferior");
        } else if (actualTemp >= tempMax) {
            // Lançar ALERTA??? ou função WriteMySQL???
            System.out.println("Temperatura atingiu limite superior");
        }

    }


    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Temps_Migration");
        
        // Immediately launches the Thread: Thread_Experiment_Watcher 
        new ExperimentWatcher().start();

    }
}
