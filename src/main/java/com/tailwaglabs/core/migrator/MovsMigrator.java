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
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;

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

    TopologyService topologyService = null;

    Logger logger = null;

    long movsTimestamp = System.currentTimeMillis() - 60 * 1000; // To get movement readings that might happen seconds before this thread starts running
    private List<Boolean> lastTenReadings = new ArrayList<>();

    private TimerTask createMiceStoppedTask() {
        return new TimerTask() {
            @Override
            public void run() {
                try {
                    watcher.alertMovementAbsence(watcher.getSecondsWithoutMovement());
                } catch (SQLException e) {
                    e.printStackTrace();
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
            { Timestamp: { $gte: %d }, Migrated: { $ne: 1 } }
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
        topologyService = new TopologyService();
        try {
            ExperimentWatcher.setMovsMigratorInstance(this);
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
            connectToMariaDB();
        } catch (SQLException e) {
            logger.log("Error connecting to MariaDB." + e);
        } catch (IOException e) {
            logger.log("Error reading config.ini file." + e);
        }
        currentTimestamp = System.currentTimeMillis();
    }

    public void connectToMariaDB() throws SQLException {
        mariadbConnection = DriverManager.getConnection(sqlConnection, sqlusername, sqlPassword);
    }

    private void migrationLoop() throws SQLException {
        int[][] topology = topologyService.getTopology(); // get labyrinth topology from relational
        int nbRooms = topology.length - 1;
        int miceLimit = watcher.getMiceLimit();
        int startingMiceNumber = watcher.getStartingMiceNumber();
        topologyService.show_matrix(topology); // show labyrinth topology retrieved from relational
        rooms_population.put(1, startingMiceNumber); // set starting mice number in 1st room (HASHMAP)
        for (int i = 2; i <= 15; i++) { // fills 15 rooms with 0 mice (HASHMAP) - just in case ...
            rooms_population.put(i, 0);
        }
        persistInitMicePopulation(startingMiceNumber, watcher.getIdExperiment(), nbRooms);
        startTimer(); // timer to keep track of mice movement
        while (TempsMigrator.getExperimentRunning()) {
            // Connection lost...
            if (mariadbConnection == null) {
                try {
                    logger.log("Not connected to MariaDB.");
                    logger.log("--- Sleeping " + (MOVS_FREQUENCY / 1000) + " seconds... ---\n");
                    //noinspection BusyWait
                    Thread.sleep(MOVS_FREQUENCY);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
            String query = String.format(QUERY_MONGO_GET_MOVS, movsTimestamp);
            FindIterable<Document> results = movsCollection.find(BsonDocument.parse(query));
            Document doc = null;
            Iterator<Document> cursor = results.iterator();
            while (cursor.hasNext()) {
                boolean illegalMovement = false;
                doc = cursor.next();
                int from_room = doc.getInteger("SalaOrigem");
                int to_room = doc.getInteger("SalaDestino");
                if (from_room > nbRooms || to_room > nbRooms || from_room == to_room || topology[from_room][to_room] == 0 || rooms_population.get(from_room) == 0) {
                    invalidMovement(from_room, to_room);
                    illegalMovement = true;
                } else {  // movement can be performed
                    stopTimer();  // reset timer to keep track of mice movement
                    startTimer(); // reset timer to keep track of mice movement
                    rooms_population.put(to_room, rooms_population.get(to_room) + 1);
                    rooms_population.put(from_room, rooms_population.get(from_room) - 1);
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
                    for (var entry : rooms_population.entrySet()) {
                        persistMicePopulation(entry.getKey(), entry.getValue(), watcher.getIdExperiment());
                    }
                }
                persistMov(doc, watcher.getIdExperiment(), illegalMovement);
                logger.log("Mice in rooms: " + rooms_population);
            }
            if (doc != null) {
                movsTimestamp = System.currentTimeMillis() - 1000;
            }
            //logger.log("--- Sleeping " + (MOVS_FREQUENCY / 1000) + " seconds... ---\n"); //REINSTATE
            try {
                Thread.sleep(MOVS_FREQUENCY);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void invalidMovement(int from_room, int to_room) {
        try {
            String message = "Movimento ilegal detetado entre a sala %d e a sala %d.";
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
        } catch (Exception e) {
            logger.log(e);
        }
    }

    private boolean validateReading(Document doc) { // returns true with valid readings
        String dateFromMongo = doc.getString("Hora");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long hora = 0;
        try {
            Date date = formatter.parse(dateFromMongo);
            hora = date.getTime();
        } catch (Exception e) {
            logger.log(e);
        }
        long timeStamp = doc.getLong("Timestamp");
        long fifteenMinutesAgo = timeStamp - (15 * 60 * 1000);
        return doc.containsKey("SalaDestino") &&
                doc.containsKey("SalaOrigem") &&
                doc.containsKey("Hora") &&
                doc.containsKey("Timestamp") &&
                (hora > fifteenMinutesAgo) && // not valid because reading was more than 15 minutes ago
                (hora < timeStamp); // not valid because reading was in the future
    }

    public void checkTooManyErrors() throws SQLException {
        if (lastTenReadings.size() <= 10) {
            return;
        }
        int errorLimit = 0;
        for (Boolean error : lastTenReadings) {
            if (!error) {
                errorLimit++;
            }
        }
        lastTenReadings.remove(0);
        if (errorLimit >= 5) {
            logger.log(Logger.Severity.DANGER, "Sending too many errors Alert");
            try {
                PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
                statement.setLong(1, watcher.getIdExperiment());
                statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                statement.setNull(3, Types.INTEGER);
                statement.setNull(4, Types.DOUBLE);
                statement.setInt(5, AlertType.AVISO.getValue());
                String message = "Sensores de movimento registaram demasiados erros.";
                statement.setString(6, message);
                statement.setInt(7, AlertSubType.SENSOR_ERRORS.getValue());
                statement.setNull(8, Types.INTEGER);
                statement.executeUpdate();
            } catch (Exception e) {
                logger.log(Logger.Severity.WARNING, "Error persisting errors alert");
            }
        }
    }

    private boolean existsExperience(long exp, int nbRooms) {
        int nbRows = 0;
        try {
            Statement s = mariadbConnection.createStatement();
            String sqlQuery = String.format("SELECT COUNT (*) FROM salas_ratos WHERE experiencia = %s", exp);
            ResultSet resultSet = s.executeQuery(sqlQuery);
            resultSet.next();
            nbRows = resultSet.getInt(1); // rows will be 10 if experience already exists
        } catch (Exception e) {
            logger.log("Error querying database . " + e);
        }
        return nbRows == nbRooms;
    }

    private void persistInitMicePopulation(int nbMice, long exp, int nbRooms) { // init the rooms in relational
        String sqlInsert = "";
        String sqlUpdate = "";
        boolean existsExperience = existsExperience(exp, nbRooms);
        try {
            Statement s = mariadbConnection.createStatement();
            sqlInsert = String.format("""
                    INSERT INTO salas_ratos(sala, ratos, experiencia)
                    VALUES (%d, %d, %d)
                    """, 1, nbMice, exp);
            sqlUpdate = String.format("""
                    UPDATE salas_ratos set ratos = %d
                    WHERE sala = 1 and experiencia = %d
                    """, nbMice, exp);
            if (existsExperience) {
                s.executeUpdate(sqlUpdate);
            } else {
                s.executeUpdate(sqlInsert);
            }
            for (int sala = 2; sala <= nbRooms; sala++) {
                sqlInsert = String.format("""
                        INSERT INTO salas_ratos(sala, ratos, experiencia)
                        VALUES (%d, 0, %d)
                        """, sala, exp);
                sqlUpdate = String.format("""
                    UPDATE salas_ratos set ratos = 0
                    WHERE sala = %d and experiencia = %d
                    """, sala, exp);
                if (existsExperience) {
                    s.executeUpdate(sqlUpdate);
                } else {
                    s.executeUpdate(sqlInsert);
                }
            }
            s.close();
        } catch (Exception e) {
            logger.log("Error Inserting in the database. " + e);
            logger.log(sqlInsert);
            logger.log(sqlUpdate);
        }
    }

    private void persistMicePopulation(int room, int nbMice, long exp) {
        String sqlQuery = String.format("""
                UPDATE salas_ratos SET ratos = %d WHERE sala = %d AND experiencia = %d
                """, nbMice, room, exp);
        try {
            Statement s = mariadbConnection.createStatement();
            s.executeUpdate(sqlQuery);
            s.close();
        } catch (Exception e) {
            logger.log("Error Inserting in the database . " + e);
            logger.log(sqlQuery);
        }
    }

    private void persistMov(Document doc, long experiencia, boolean illegalMovement) {
        int salaOrigem = doc.getInteger("SalaOrigem");
        int salaDestino = doc.getInteger("SalaDestino");
        LocalDateTime hora = LocalDateTime.parse(doc.getString("Hora").replace(" ", "T"));
        long timestamp = doc.getLong("Timestamp");
        int isError = validateReading(doc) ? 0 : 1; // if 0 there is no error
        Boolean isValidReading = (isError == 0);
        if (illegalMovement) {
            isValidReading = false;
        }
        logger.log("Valid reading: " + isValidReading);
        lastTenReadings.add(isValidReading);
        try {
            checkTooManyErrors();
        } catch (SQLException e) {
            logger.log("Error connecting to MariaDB." + e);
        }
        String sqlQuery = String.format("""
                INSERT INTO medicoespassagens(IDExperiencia, SalaOrigem, SalaDestino, Hora, TimestampRegisto, IsError)
                VALUES (%d, %d, %d, '%s', FROM_UNIXTIME(%d / 1000), %d)
                """, experiencia, salaOrigem, salaDestino, hora, timestamp, isError);
        try {
            // Inserção no MySQL
            Statement s = mariadbConnection.createStatement();
            int rowsChanged = s.executeUpdate(sqlQuery);
            s.close();
            if (rowsChanged > 0) {
                Document filter = new Document("_id", doc.get("_id"));
                Document update = new Document("$set", new Document("Migrated", 1));
                movsCollection.updateOne(filter, update);
                logger.log("Movs Migration successful and MongoDB updated.");
            }
        } catch (Exception e) {
            logger.log("Error Inserting in the database: " + doc.get("_id"));
            logger.log(sqlQuery);
        }
    }

    public void setMariadbConnection(Connection mariadbConnection) {
        this.mariadbConnection = mariadbConnection;
    }
}
