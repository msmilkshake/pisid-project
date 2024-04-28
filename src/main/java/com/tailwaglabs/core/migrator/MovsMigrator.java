package com.tailwaglabs.core.migrator;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.AlertSubType;
import com.tailwaglabs.core.AlertType;
import com.tailwaglabs.core.ExperimentWatcher;
import com.tailwaglabs.core.Logger;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Migrates and processes Movs sensor data: Mongo - MySQL
 * Executes - When an experiment is started (Django button starts this)
 */
public class MovsMigrator extends Thread {

    private final boolean LOGGER_ENABLED = true;

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
    Timer timer = null;
    TimerTask miceStoppedTask = null;

    Logger logger = null;

    long movsTimestamp = System.currentTimeMillis();

    private TimerTask createMiceStoppedTask() {
        return new TimerTask() {
            @Override
            public void run() {
                try {
                    watcher.alertMovementAbsence(watcher.getSecondsWithoutMovement());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private void startTimer() {
        miceStoppedTask = createMiceStoppedTask();
        timer = new Timer();
        long delay = watcher.getSecondsWithoutMovement() * 1000L;
        timer.schedule(miceStoppedTask, delay);
    }

    private void stopTimer() {
        miceStoppedTask.cancel();
        timer.cancel();
        timer.purge();
        timer = null;
        miceStoppedTask = null;
    }
    private final int MOVS_FREQUENCY = 1 * 1000; // 1 second

    private HashMap<Integer, Integer> rooms_population = new HashMap<>();

    private final String QUERY_MONGO_GET_MOVS = """
            { Timestamp: { $gte: %d }, Migrated: { $ne: '1' } }
            """;

    private final String QUERY_SQL_INSERT_ALERT = """ 
            INSERT INTO alerta(IDExperiencia, Hora, SalaOrigem, SalaDestino, TipoAlerta, Mensagem, SubTipoAlerta, Sala)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    public void run() {
        init();
        try {
            migrationLoop();
        } catch (SQLException e) {
            logger.log("Error connecting to MariaDB." + e);
        }
    }

    private void init() {
        Thread.currentThread().setName("Main_Movs_Migration");
        logger = new Logger("Main_Movs_Migration", Logger.TextColor.CYAN);
        logger.setEnabled(LOGGER_ENABLED);
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
            logger.log("Error connecting to MariaDB." + e);
        } catch (IOException e) {
            logger.log("Error reading config.ini file." + e);
        }
        currentTimestamp = System.currentTimeMillis();
    }

    private void migrationLoop() throws SQLException {
        int[][] topology = new ConnectToMysql().getTopology(); // get labyrinth topology from relational
        int miceLimit = watcher.getMiceLimit();
        ConnectToMysql.show_matrix(topology); // show labyrinth topology retrieved from relational
        rooms_population.put(1, watcher.getStartingMiceNumber()); // set initial mice number in 1st room
        for (int i = 2; i <= 10; i++) { // remaining 9 rooms with 0 mice
            rooms_population.put(i, 0);
        }
        startTimer();

        while (TempsMigrator.getExperimentRunning()) {
            String query = String.format(QUERY_MONGO_GET_MOVS, movsTimestamp);
            FindIterable<Document> results = movsCollection.find(BsonDocument.parse(query));
            Document doc = null;
            Iterator<Document> cursor = results.iterator();

            while (cursor.hasNext()) {
                doc = cursor.next();
                int from_room = doc.getInteger("SalaOrigem");
                int to_room = doc.getInteger("SalaDestino");
                if (from_room == to_room || topology[from_room][to_room] == 0 || rooms_population.get(from_room) == 0) {
                    String message = "Movimento ilegal detetado entre a Sala %d e a sala %d.";
                    message = String.format(message, from_room, to_room);
                    PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
                    statement.setLong(1, watcher.getIdExperiment());
                    statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                    statement.setInt(3, from_room);
                    statement.setInt(4, to_room);
                    statement.setInt(5, AlertType.AVISO.getValue());
                    statement.setString(6, message);
                    statement.setInt(7, AlertSubType.ILLEGAL_MOVEMENT.getValue());
                    statement.setNull(8, Types.NULL);
                    statement.executeUpdate();
                    statement.close();
                    logger.log("ALERT: invalid movement!");
                } else {  // movement can be performed
                    stopTimer();
                    startTimer();
                    rooms_population.put(to_room, rooms_population.get(to_room) + 1);
                    rooms_population.put(from_room, rooms_population.get(from_room) - 1);
                    persistMov(doc, System.currentTimeMillis(), watcher.getIdExperiment());
                    if (rooms_population.get(to_room) >= miceLimit) { // Alert if mice number exceeded limit
                        String message = "Excesso de ratos na Sala %d.";
                        message = String.format(message, to_room);
                        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
                        statement.setLong(1, watcher.getIdExperiment());
                        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                        statement.setNull(3, Types.NULL);
                        statement.setNull(4, Types.NULL);
                        statement.setInt(5, AlertType.AVISO.getValue());
                        statement.setString(6, message);
                        statement.setInt(7, AlertSubType.MAX_MICE_REACHED.getValue());
                        statement.setInt(8, to_room);
                        statement.executeUpdate();
                        statement.close();
                        logger.log("ALERT TOO MANY MICE in room " + to_room);
                    }
                }
                logger.log("Mice in rooms: " + rooms_population);
            }
            if (doc != null) {
                movsTimestamp = System.currentTimeMillis();
            }
//            logger.log("--- Sleeping " + (MOVS_FREQUENCY / 1000) + " seconds... ---\n"); // REINSTATE
            try {
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
            s.executeUpdate(sqlQuery);
            s.close();
        } catch (Exception e) {
            logger.log("Error Inserting in the database . " + e);
            logger.log(sqlQuery);
        }
    }

//    public static void main(String[] args) throws SQLException {
//        Thread.currentThread().setName("Main_Movs_Migration");
//        new MovsMigrator().run();
//    }
}
