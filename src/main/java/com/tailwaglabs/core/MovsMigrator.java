package com.tailwaglabs.core;

import com.mongodb.Mongo;

public class MovsMigrator {

    /**
     * Migrates and processes Movs sensor data: Mongo - MySQL
     * Executes - When an experiment is started (Django button starts this)
     */    
    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Movs_Migration");
        
    }
    
}
