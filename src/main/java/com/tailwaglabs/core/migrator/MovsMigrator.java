package com.tailwaglabs.core.migrator;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.ExperimentWatcher;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Migrates and processes Movs sensor data: Mongo - MySQL
 * Executes - When an experiment is started (Django button starts this)
 */
public class MovsMigrator {

    private ExperimentWatcher watcher = ExperimentWatcher.getInstance();

    // ini Mongo Properties
    private String mongoHost = "localhost";
    private int mongoPort = 27019;
    private String mongoDatabase = "mqqt";
    private String mongoCollection = "movs";

    // ini MariaDB Properties
    private String sqlConnection = "";
    private String sqlusername = "";
    private String sqlPassword = "";

    private MongoClient mongoClient;
    private MongoDatabase db;
    private MongoCollection<Document> tempsCollection;
    private long currentTimestamp;
    private MongoCursor<Document> cursor;
    private Connection mariadbConnection;
    private boolean isBatchMigrated = true;
    private final int MAX_NUMBER_OF_OUTLIERS = 3;

    private final int MOVS_FREQUENCY = 10 * 1000; // 3 seconds

    private final String QUERY_MONGO_GET_MOVS = """
            { Timestamp: { $gte: %d }, Migrated: { $ne: '1' } }
            """;


    private void run() {
        init();
        migrationLoop();
    }

    private void init() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("config.ini"));

            mongoHost = p.getProperty("mongo_host");
            mongoPort = Integer.parseInt(p.getProperty("mongo_port"));
            mongoDatabase = p.getProperty("mongo_database");
            mongoCollection = p.getProperty("collection_temps");

            sqlConnection = p.getProperty("sql_database_connection_to");
            sqlPassword = p.getProperty("sql_database_password_to");
            sqlusername = p.getProperty("sql_database_user_to");

            mongoClient = new MongoClient(mongoHost, mongoPort);
            db = mongoClient.getDatabase(mongoDatabase);
            tempsCollection = db.getCollection(mongoCollection);
            mariadbConnection = DriverManager.getConnection(sqlConnection, sqlusername, sqlPassword);
        } catch (SQLException e) {
            System.out.println("Error connecting to MariaDB." + e);
        } catch (IOException e) {
            System.out.println("Error reading config.ini file." + e);
        }
        currentTimestamp = System.currentTimeMillis();
    }

    private void migrationLoop() {
        while (true) {
            Document tempsQuery = Document.parse(String.format(QUERY_MONGO_GET_MOVS, currentTimestamp));
            cursor = tempsCollection.find(tempsQuery).iterator();

            Document doc = null;
            while (cursor.hasNext()) {
                doc = cursor.next();
                System.out.println(doc);
                 // persistMov(doc); // TODO-> valida a passagem etc etc...
            }
            if (doc != null) {
                currentTimestamp = System.currentTimeMillis();
            }

            System.out.println("--- Sleeping " + (MOVS_FREQUENCY / 1000) + " seconds... ---\n");
            try {
                //noinspection BusyWait
                Thread.sleep(MOVS_FREQUENCY);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }




    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Movs_Migration");

        new MovsMigrator().migrationLoop();

    }

}
