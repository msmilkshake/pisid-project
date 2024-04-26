package com.tailwaglabs.core.migrator;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.ExperimentWatcher;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
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
    private MongoCollection<Document> movsCollection;
    private long currentTimestamp;
    private MongoCursor<Document> cursor;
    private Connection mariadbConnection;
    private boolean isBatchMigrated = true;
    private final int MAX_NUMBER_OF_OUTLIERS = 3;

    long movsTimestamp = System.currentTimeMillis();
    String sql_database_connection_to = "";
    String sql_database_password_to = "";
    String sql_database_user_to = "";

    private final int MOVS_FREQUENCY = 3 * 1000; // 3 seconds

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
            mongoCollection = p.getProperty("collection_movs");

            sqlConnection = p.getProperty("sql_database_connection_to");
            sqlPassword = p.getProperty("sql_database_password_to");
            sqlusername = p.getProperty("sql_database_user_to");

            mongoClient = new MongoClient(mongoHost, mongoPort);
            db = mongoClient.getDatabase(mongoDatabase);
            movsCollection = db.getCollection(mongoCollection);
            mariadbConnection = DriverManager.getConnection(sqlConnection, sqlusername, sqlPassword);
        } catch (SQLException e) {
            System.out.println("Error connecting to MariaDB." + e);
        } catch (IOException e) {
            System.out.println("Error reading config.ini file." + e);
        }
        connectToDatabases();
        currentTimestamp = System.currentTimeMillis();
    }

    private void migrationLoop() {
        int[][] topology = new ConnectToMysql().getTopology(); // get labyrinth topology from relational
        System.out.println("Labyrinth topology:");
        ConnectToMysql.show_matrix(topology); // show labyrinth topology retrieved from relational
        HashMap<Integer, Integer> rooms_population = new HashMap<>();
        rooms_population.put(1, 20); // 20 mice on room 1 at startup for testing purposes
        for (int i = 2 ; i <= 10; i++) { // remaining 9 rooms with 0 mice
            rooms_population.put(i, 0);
        }
        while (true) {
            String testQ = "{ Timestamp: { $gte: " + movsTimestamp + " } }";
            FindIterable<Document> results = movsCollection.find(BsonDocument.parse(testQ));
            Document doc = null;
            Iterator<Document> cursor = results.iterator();
            while (cursor.hasNext()) {
                doc = cursor.next();
                int from_room = (Integer) doc.get("SalaOrigem");
                int to_room = (Integer) doc.get("SalaDestino");
                if (from_room == to_room) {
                    System.out.println("ALERT: movement to SAME ROOM - invalid movement!");
                } else if (topology[from_room][to_room] == 0) {
                    System.out.println("ALERT: rooms NOT CONNECTED - invalid movement!");
                } else if (rooms_population.get(from_room) == 0) {
                    System.out.println("ALERT: room is EMPTY - invalid movement!");
                } else {  // movement can be performed
                    rooms_population.put(to_room, rooms_population.get(to_room) + 1);
                    rooms_population.put(from_room, rooms_population.get(from_room) - 1);
                    persistMov(doc, System.currentTimeMillis(), 1); // Value 1 is hard coded EXPERIENCE NUMBER
                }
                System.out.println("Mice in rooms: " + rooms_population);

            }
            if (doc != null) {
                movsTimestamp = System.currentTimeMillis();
            }
//            System.out.println("--- Sleeping " + (MOVS_FREQUENCY / 1000) + " seconds... ---\n"); // REINSTATE
            try {
                //noinspection BusyWait
                Thread.sleep(MOVS_FREQUENCY);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public void persistMov(Document doc, long timestamp, long experiencia) {
        int salaOrigem = doc.getInteger("SalaOrigem");
        int salaDestino = doc.getInteger("SalaDestino");
        LocalDateTime hora = LocalDateTime.parse(doc.getString("Hora").replace(" ", "T"));

        String sqlQuery = String.format("" +
                        "INSERT INTO medicoespassagens(IDExperiencia, SalaOrigem, SalaDestino, Hora, TimestampRegisto)\n" +
                        "VALUES (%d, %d, %d, '%s', FROM_UNIXTIME(%d / 1000))",
                experiencia, salaOrigem, salaDestino, hora, timestamp
        );
        try {
            Statement s = mariadbConnection.createStatement();
            int result = s.executeUpdate(sqlQuery);
            s.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database . " + e);
            System.out.println(sqlQuery);
        }
    }

    public void connectToDatabases() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            mariadbConnection = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to, sql_database_password_to);
        } catch (Exception e) {
            System.out.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
    }


    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Movs_Migration");

        MovsMigrator movsMigrator = new MovsMigrator();
        movsMigrator.run();
        movsMigrator.migrationLoop();
//        new MovsMigrator().migrationLoop();

    }

}
