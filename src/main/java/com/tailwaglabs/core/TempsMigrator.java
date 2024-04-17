package com.tailwaglabs.core;

import com.mongodb.Mongo;

public class TempsMigrator {

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
