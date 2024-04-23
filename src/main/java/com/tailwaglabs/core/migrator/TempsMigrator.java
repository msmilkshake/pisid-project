package com.tailwaglabs.core.migrator;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.AlertType;
import com.tailwaglabs.core.ExperimentWatcher;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Migrates and processes Temps sensor data: Mongo - MySQL
 * Executes - Same time as the Django app (Can be a script that runs both)
 * <p>
 * This thread only processes some data if an experiment is running
 * If no experiment is running only processes: Outliers and Incorrect data.
 */
public class TempsMigrator {

    // ini Mongo Properties
    private String mongoHost = "localhost";
    private int mongoPort = 27019;
    private String mongoDatabase = "mqqt";
    private String mongoCollection = "temps";

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

    private final String QUERY_MONGO_GET_TEMPS = """
            { Timestamp: { $gte: %d }, Migrated: { $ne: '1' } }
            """;

    private final String QUERY_SQL_INSERT_TEMP = """
            INSERT INTO medicoestemperatura(IDExperiencia, Leitura, Sensor, Hora, TimestampRegisto, isError)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
    // TODO Investigar pq é preciso dividir por 1000

    private final String QUERY_SQL_GET_TEMP_MIN_MAX = """ 
            SELECT parametrosexperiencia.TemperaturaMaxima, parametrosexperiencia.TemperaturaMinima
            FROM experiencia
            JOIN parametrosexperiencia ON experiencia.IDParametros = parametrosexperiencia.IDParametros
            WHERE experiencia.IDExperiencia = ?;
            """;

    private final String QUERY_SQL_INSERT_TEMP_ALERT = """ 
            INSERT INTO alerta(IDExperiencia, Hora, Sensor, Leitura, TipoAlerta, Mensagem)
            VALUES (?, ?, ?, ?, ?, ?)
            """;


    private boolean isExperimentRunning = false;
    private final Lock EXPERIMENT_LOCK = new ReentrantLock();
    private final int TEMPS_FREQUENCY = 3 * 1000; // 3 seconds

    private static ExperimentWatcher watcher;


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
            Document tempsQuery = Document.parse(String.format(QUERY_MONGO_GET_TEMPS, currentTimestamp));
            cursor = tempsCollection.find(tempsQuery).iterator();

            Document doc = null;
            while (cursor.hasNext()) {
                doc = cursor.next();
                System.out.println(doc);
                persistTemp(doc);
                // Teste das duas funções, OK para prints, falta insert do alert no Mysql
                try {
                    checkLimitProximity(doc);
                    limitReached(doc);
                } catch (SQLException e) {
                    System.out.println("Error connecting to MariaDB." + e);
                }
            }
            if (doc != null) {
                currentTimestamp = System.currentTimeMillis();
            }

            System.out.println("--- Sleeping " + (TEMPS_FREQUENCY / 1000) + " seconds... ---\n");
            try {
                //noinspection BusyWait
                Thread.sleep(TEMPS_FREQUENCY);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void persistTemp(Document doc) {
        Boolean validReading = validateReading(doc);
        Double reading = doc.getDouble("Leitura");
        Integer sensor = doc.getInteger("Sensor");
        Long recordedTimestamp = doc.getLong("Timestamp");
        String readingTimeStr = doc.getString("Hora").replace(" ", "T");
        LocalDateTime readingTime = readingTimeStr != null ?
                LocalDateTime.parse(readingTimeStr) :
                null;

        Long experimentId = 1L;

        try {
            PreparedStatement statement =
                    mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP, PreparedStatement.RETURN_GENERATED_KEYS);
            if (experimentId == null) {
                statement.setNull(1, Types.INTEGER);
            } else {
                statement.setLong(1, experimentId);
            }
            if (reading == null) {
                statement.setNull(2, Types.DECIMAL);
            } else {
                statement.setDouble(2, reading);
            }
            if (sensor == null) {
                statement.setNull(3, Types.INTEGER);
            } else {
                statement.setInt(3, sensor);
            }
            if (readingTime == null) {
                statement.setNull(4, Types.TIMESTAMP);
            } else {
                statement.setTimestamp(4, Timestamp.valueOf(readingTime));
            }
            if (recordedTimestamp == null) {
                statement.setNull(5, Types.BIGINT);
            } else {
                statement.setLong(5, recordedTimestamp);
            }
            statement.setInt(6, validReading ? 0 : 1);

            statement.executeUpdate();
            ResultSet generatedKeys = statement.getGeneratedKeys();

            if (generatedKeys.next()) {
                long newId = generatedKeys.getLong(1);
                System.out.println("Generated ID: " + newId);
            } else {
                throw new SQLException("Creating record failed, no ID obtained.");
            }
            statement.close();
        } catch (Exception e) {
            System.out.println("Error Inserting in the database. " + e);
        }
    }

    private boolean validateReading(Document doc) {
        return doc.containsKey("Leitura") &&
               doc.containsKey("Sensor") &&
               doc.containsKey("Hora") &&
               doc.containsKey("Timestamp");
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

    public void checkLimitProximity(Document doc) throws SQLException {

        if(!isExperimentRunning) {
            return;
        }

        double actualTemp = doc.getDouble("Leitura");
        int sensor = doc.getInteger("Sensor");

        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_GET_TEMP_MIN_MAX);
        statement.setInt(1, watcher.getIdExperiment());
        ResultSet resultSet = statement.executeQuery();

        double tempMin =  0;
        double tempMax = 0;

        if(resultSet.next()) {
            tempMin =  resultSet.getDouble("TemperaturaMinima");
            tempMax = resultSet.getDouble("TemperaturaMaxima");
        }

        double range = tempMax - tempMin;

        double minLimit = tempMin + 0.1 * range;
        double maxLimit = tempMin + 0.9 * range;

        String message = "Temperatura próxima do limite %s definido no sensor %d.";

        statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setInt(1,watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, sensor);
        statement.setDouble(4, actualTemp);
        statement.setInt(5, AlertType.AVISO.getValue());

        if (actualTemp <= minLimit && actualTemp >= tempMin) {
            message = String.format(message, "inferior", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            System.out.println("Temperatura próxima do limite inferior");
        } else if (actualTemp >= maxLimit && actualTemp <= tempMax) {
            message = String.format(message, "superior", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            System.out.println("Temperatura próxima do limite superior");
        }
        statement.close();
    }

    public void limitReached(Document doc) throws SQLException {

        if(!isExperimentRunning) {
            return;
        }

        double actualTemp = doc.getDouble("Leitura");
        int sensor = doc.getInteger("Sensor");

        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_GET_TEMP_MIN_MAX);
        statement.setInt(1, watcher.getIdExperiment());
        ResultSet resultSet = statement.executeQuery();

        double tempMin =  0;
        double tempMax = 0;

        if(resultSet.next()) {
            tempMin =  resultSet.getDouble("TemperaturaMinima");
            tempMax = resultSet.getDouble("TemperaturaMaxima");

        }

        String message = "Temperatura atingiu o limite %s definido no sensor %d.";

        statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setInt(1,watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, sensor);
        statement.setDouble(4, actualTemp);
        statement.setInt(5, AlertType.PERIGO.getValue());

        if (actualTemp <= tempMin) {
            message = String.format(message, "mínimo", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            System.out.println("Temperatura atingiu limite inferior");
        } else if (actualTemp >= tempMax) {
            message = String.format(message, "máximo", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            System.out.println("Temperatura atingiu limite superior");
        }

        statement.close();

    }

    public void setExperimentRunning(boolean isRunning) {
        synchronized (EXPERIMENT_LOCK) {
            try {
                EXPERIMENT_LOCK.lock();
                isExperimentRunning = isRunning;
            } finally {
                EXPERIMENT_LOCK.unlock();
            }
        }
    }


    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Temps_Migration");

        // Immediately launches the Thread: Thread_Experiment_Watcher 
        watcher = new ExperimentWatcher();
        watcher.start();
        new TempsMigrator().run();

    }

}
