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
    private final int MAX_NUMBER_OF_OUTLIERS = 3;

    private final String QUERY_MONGO_GET_TEMPS = """
            { Timestamp: { $gte: %d }, Migrated: { $ne: '1' } }
            """;

    private final String QUERY_SQL_INSERT_TEMP = """
            INSERT INTO medicoestemperatura(IDExperiencia, Leitura, Sensor, Hora, TimestampRegisto, isError, isOutlier)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
    // TODO Investigar pq é preciso dividir por 1000

    private final String QUERY_SQL_INSERT_TEMP_ALERT = """ 
            INSERT INTO alerta(IDExperiencia, Hora, Sensor, Leitura, TipoAlerta, Mensagem)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private final String QUERY_SQL_GET_LAST_X_RECORDS = """ 
            SELECT * FROM medicoestemperatura
            WHERE IsOutlier = 0
                AND Sensor = ?
            ORDER BY TimestampRegisto DESC
            LIMIT ?;
            """;

    private final String QUERY_SQL_COUNT_OUTLIERS_LAST_X_RECORDS = """ 
            SELECT COUNT(*) AS outliers_count
            FROM (
                SELECT IsOutlier
                FROM medicoestemperatura
                WHERE Sensor = ?
                ORDER BY TimestampRegisto DESC
                LIMIT ?
            ) AS ultimos_registros
            WHERE IsOutlier = 1;
            """;

    private boolean isExperimentRunning = true;
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

        boolean isOutlier = false;

        // Faz as validações para saber se lança os alertas
        // mas os registo são inseridos na mesma no Mysql
        // Se falha alguma coisa insere alerta mas não insere o registo??
        try {
            checkLimitProximity(doc);
            limitReached(doc);
            isOutlier = isOutlier(doc);
            sendTooManyOutliersAlert(doc);
        } catch (SQLException e) {
            System.out.println("Error connecting to MariaDB." + e);
        }

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

            statement.setInt(7, isOutlier ? 1 : 0);

            statement.executeUpdate();
            ResultSet generatedKeys = statement.getGeneratedKeys();

            if (generatedKeys.next()) {
                long newId = generatedKeys.getLong(1);
                //System.out.println("Generated ID: " + newId);
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

    public void checkLimitProximity(Document doc) throws SQLException {

        if(!isExperimentRunning) {
            return;
        }

        double actualTemp = doc.getDouble("Leitura");
        int sensor = doc.getInteger("Sensor");

        double tempMin = watcher.getExperimentMinTemp();
        double tempMax = watcher.getExperimentMaxTemp();

        double range = tempMax - tempMin;

        double minLimit = tempMin + 0.1 * range;
        double maxLimit = tempMin + 0.9 * range;

        String message = "Temperatura próxima do limite %s definido no sensor %d.";

        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setInt(1,watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, sensor);
        statement.setDouble(4, actualTemp);
        statement.setInt(5, AlertType.AVISO.getValue());

        if (actualTemp <= minLimit && actualTemp >= tempMin) {
            message = String.format(message, "inferior", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            //System.out.println("Temperatura próxima do limite inferior");
        } else if (actualTemp >= maxLimit && actualTemp <= tempMax) {
            message = String.format(message, "superior", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            //System.out.println("Temperatura próxima do limite superior");
        }
        statement.close();
    }

    public void limitReached(Document doc) throws SQLException {

        if(!isExperimentRunning) {
            return;
        }

        double actualTemp = doc.getDouble("Leitura");
        int sensor = doc.getInteger("Sensor");

        double tempMin = watcher.getExperimentMinTemp();
        double tempMax = watcher.getExperimentMaxTemp();

        String message = "Temperatura atingiu o limite %s definido no sensor %d.";

        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setInt(1,watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, sensor);
        statement.setDouble(4, actualTemp);
        statement.setInt(5, AlertType.PERIGO.getValue());

        if (actualTemp <= tempMin) {
            message = String.format(message, "mínimo", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            //System.out.println("Temperatura atingiu limite inferior");
        } else if (actualTemp >= tempMax) {
            message = String.format(message, "máximo", sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            //System.out.println("Temperatura atingiu limite superior");
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

    public boolean isOutlier(Document doc) throws SQLException {

        // Variable to be returned
        boolean isOutlier = false;

        // Get the Sensor
        int sensor = doc.getInteger("Sensor");

        // Actual temperature from mongo record
        double actualTemp = doc.getDouble("Leitura");

        // Variable to store the average temperature
        double averageTemp = 0;

        // Number of records to get from the db, value defined in each experiment
        int numberOfRecords = watcher.getOutlierRecordsNumber();

        // Query the db to get the last X records that are not outliers
        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_GET_LAST_X_RECORDS);
        statement.setInt(1, sensor);
        statement.setInt(2, numberOfRecords);
        ResultSet resultSet = statement.executeQuery();


        /*
        int results = 0;

        if (resultSet != null) {
            resultSet.last(); // moves cursor to the last row
            results = resultSet.getRow(); // get row number which is equal to the total number of rows
            resultSet.beforeFirst(); // moves the cursor back to before the first row
        }

        System.out.println("Results " + results);
         */



        // Add all the temperatures of the X records
        while (resultSet.next()) {
            averageTemp += resultSet.getDouble("Leitura");
        }

        // Divide by the number of records (X)
        averageTemp /= numberOfRecords;


        if(sensor == 2) {
            System.out.println("MIN " + (averageTemp - watcher.getOutlierTempMaxVar()) + " ; MAX "+(averageTemp + watcher.getOutlierTempMaxVar()));

            System.out.println("THIS IS THE ACTUAL TEMP " + actualTemp + "THIS IS THE AVERAGE " + averageTemp + "Number of Records " + numberOfRecords);
        }



        // If the actual temp is greater than the average + the value OutlierVariacaoTempMax then is outlier
        // Or if the actual temp is lower than the average - the value OutlierVariacaoTempMax then is outlier
        if(actualTemp > averageTemp + watcher.getOutlierTempMaxVar() || actualTemp < averageTemp - watcher.getOutlierTempMaxVar()) {
            isOutlier = true;
        }

        return isOutlier;
    }

    public void sendTooManyOutliersAlert(Document doc) throws SQLException {

        if(!isExperimentRunning) {
            return;
        }

        int sensor = doc.getInteger("Sensor");
        double actualTemp = doc.getDouble("Leitura");

        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_COUNT_OUTLIERS_LAST_X_RECORDS);
        statement.setInt(1, sensor);
        statement.setInt(2, watcher.getOutlierRecordsNumber());
        //System.out.println("Number of records " + watcher.getOutlierRecordsNumber());
        ResultSet resultSet = statement.executeQuery();
        int numberOfOutliers = 0;

        if(resultSet.next()) {
            numberOfOutliers =  resultSet.getInt( "outliers_count");
            //System.out.println("Count of outliers " + numberOfOutliers);
        }

        String message = "Sensor de temperatura %d registou demasiados Outliers.";

        if(numberOfOutliers > MAX_NUMBER_OF_OUTLIERS) {
            statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
            statement.setInt(1,watcher.getIdExperiment());
            statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            statement.setInt(3, sensor);
            statement.setDouble(4, actualTemp);
            statement.setInt(5, AlertType.INFORMATIVO.getValue());
            message = String.format(message, sensor);
            statement.setString(6, message);
            statement.executeUpdate();
            statement.close();
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
