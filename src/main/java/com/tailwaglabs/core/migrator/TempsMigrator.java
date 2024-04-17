package com.tailwaglabs.core.migrator;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.ExperimentWatcher;
import org.bson.Document;

public class TempsMigrator {

    MongoCollection<Document> tempsCollection;

    /**
     * Migrates and processes Temps sensor data: Mongo - MySQL
     * Executes - Same time as the Django app (Can be a script that runs both)
     * 
     * This thread only processes some data if an experiment is running
     * If no experiment is running only processes: Outliers and Incorrect data.
     */
    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Temps_Migration");
        
        // Immediately launches the Thread: Thread_Experiment_Watcher 
        new ExperimentWatcher().start();

    }
}
