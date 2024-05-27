package com.tailwaglabs.core.migrator;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.AlertSubType;
import com.tailwaglabs.core.AlertType;
import com.tailwaglabs.core.ExperimentWatcher;
import com.tailwaglabs.core.Logger;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.*;
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

    private final boolean LOGGER_ENABLED = true;

    // ini Mongo Properties
    private String mongoHost = "localhost";
    private int mongoPort = 27019;
    private String mongoReplica = "sensorreplicaset";
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

    private Map<Integer, Queue<Double>> sensorReadingsQueues = new HashMap<>();
    private Map<Integer, Queue<Double>> sensorReadingsQueuesWithOutliers = new HashMap<>();

    Logger logger = null;

    private final String QUERY_MONGO_GET_TEMPS = """
            { Timestamp: { $gte: %d }, Migrated: { $ne: '1' } }
            """;

    private final String QUERY_SQL_PROCEDURE_TEMP_VARIATION = """
            CALL VerificacaoVariacaoBrusca(?);
            """;

    private final String QUERY_SQL_INSERT_TEMP = """
            INSERT INTO medicoestemperatura(IDExperiencia, Leitura, Sensor, Hora, TimestampRegisto, isError, isOutlier)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private final String QUERY_SQL_INSERT_TEMP_ALERT = """ 
            INSERT INTO alerta(IDExperiencia, Hora, Sensor, Leitura, TipoAlerta, Mensagem, SubTipoAlerta)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private final String QUERY_SQL_GET_LAST_X_RECORDS = """ 
            SELECT * FROM medicoestemperatura
            WHERE IsOutlier = 0
                AND Sensor = ?
                AND TimestampRegisto < ?
                AND TimestampRegisto > ?
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

    private static boolean isExperimentRunning = false;
    private static final Lock EXPERIMENT_LOCK = new ReentrantLock();


    private final int TEMPS_FREQUENCY = 3 * 1000; // 3 seconds

    private final int TEMPS_ABSENCE_LIMIT = 5 * 1000; // 5 seconds

    private ExperimentWatcher watcher = ExperimentWatcher.getInstance();

    private List<Boolean> lastTenReadings = new ArrayList<>();

    Timer timer = null;
    TimerTask temperatureAbsenceTask = null;


    private void run() {
        init();
        startTimer();
        migrationLoop();
    }

    public void restartQueues() {
        sensorReadingsQueues.put(1, new ArrayDeque<>());
        sensorReadingsQueues.put(2, new ArrayDeque<>());
        sensorReadingsQueuesWithOutliers.put(1, new ArrayDeque<>());
        sensorReadingsQueuesWithOutliers.put(2, new ArrayDeque<>());
    }

    private void init() {
        Thread.currentThread().setName("Main_Temps_Migration");
        logger = new Logger("Main_Temps_Migration", Logger.TextColor.GREEN);
        logger.setEnabled(LOGGER_ENABLED);
        try {
            ExperimentWatcher.setTempsMigratorInstance(this);

            restartQueues();
            Properties p = new Properties();
            p.load(new FileInputStream("config.ini"));

            mongoHost = p.getProperty("mongo_address");
            mongoReplica = p.getProperty("mongo_replica");
            mongoPort = Integer.parseInt(p.getProperty("mongo_port"));
            mongoDatabase = p.getProperty("mongo_database");
            mongoCollection = p.getProperty("collection_temps");

            sqlConnection = p.getProperty("sql_database_connection_to");
            sqlPassword = p.getProperty("sql_database_password_to");
            sqlusername = p.getProperty("sql_database_user_to");

            mongoClient = new MongoClient(mongoHost, mongoPort);
            db = connectToMongo();
            tempsCollection = db.getCollection(mongoCollection);
            connectToMariaDB();

        } catch (SQLException e) {
            logger.log("Error connecting to MariaDB." + e);

        } catch (IOException e) {
            logger.log("Error reading config.ini file." + e);

        }
        currentTimestamp = System.currentTimeMillis();
    }

    public MongoDatabase connectToMongo() {
        String mongoURI = "mongodb://";
        mongoURI = mongoURI + mongoHost;
        if (!mongoReplica.equals("false"))
            mongoURI = mongoURI + "/?replicaSet=" + mongoReplica;

        return new MongoClient(new MongoClientURI(mongoURI)).getDatabase(mongoDatabase);
    }

    public void connectToMariaDB() throws SQLException {
        mariadbConnection = DriverManager.getConnection(sqlConnection, sqlusername, sqlPassword);
    }

    private void migrationLoop() {
        while (true) {
            try {
                // Connection lost...
                if (mariadbConnection == null) {
                    try {
                        logger.log("Not connected to MariaDB.");
                        logger.log("--- Sleeping " + (TEMPS_FREQUENCY / 1000) + " seconds... ---\n");
                        // noinspection BusyWait
                        Thread.sleep(TEMPS_FREQUENCY);
                    } catch (InterruptedException e) {
                        logger.log(e);
                    }
                    continue;
                }


                Document tempsQuery = Document.parse(String.format(QUERY_MONGO_GET_TEMPS, currentTimestamp));
                cursor = tempsCollection.find(tempsQuery).iterator();

                if (cursor.hasNext()) {
                    stopTimer();
                    // System.out.println("Timer Reset");
                    startTimer();
                }


                Document doc = null;
                while (cursor.hasNext()) {
                    doc = cursor.next();
                    logger.log(doc);

                    // Se guardar com sucesso ir ao Mongo e colocar o campo deste registo com Migrated: 1
                    if (persistTemp(doc)) {
                        try {
                            Document filter = new Document(new Document("_id", doc.get("_id")));
                            Document update = new Document("$set", new Document("Migrated", "1"));
                            tempsCollection.updateOne(filter, update);

                            logger.log(Logger.Severity.INFO, "Temps Migration successful and MongoDB updated.");
                        } catch (Exception e) {
                            logger.log(Logger.Severity.WARNING, "Update for document with id:" + doc.get("_id"));
                            System.out.println("Update for document with id: " + doc.get("_id"));
                            e.printStackTrace();
                        }
                    } else {
                        logger.log(Logger.Severity.WARNING, "Temp persist failed");
                    }
                }

                if (doc != null) {
                    currentTimestamp = System.currentTimeMillis() - 15000;
                }

                logger.log("--- Sleeping " + (TEMPS_FREQUENCY / 1000) + " seconds... ---\n");
                try {
                    // noinspection BusyWait
                    Thread.sleep(TEMPS_FREQUENCY);
                } catch (InterruptedException e) {
                    logger.log(e);
                }
            } catch (MongoNotPrimaryException e) {
                logger.log(Logger.Severity.WARNING, "Mongo PRIMARY server unaccessible.");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                db = connectToMongo();
            }
        }
    }

    public boolean persistTemp(Document doc) {

        Boolean isValidReading = validateReading(doc);
        logger.log("Valid reading: " + isValidReading);
        lastTenReadings.add(isValidReading);

        try {
            checkTooManyErrors();
        } catch (SQLException e) {
            logger.log("Error connecting to MariaDB." + e);
        }

        Double reading = null;
        try {
            reading = doc.getDouble("Leitura");
        } catch (Exception e) {
            logger.log("Error getting reading \"Leitura\". " + e);
        }
        Integer sensor = null;
        try {
            sensor = doc.getInteger("Sensor");
        } catch (Exception e) {
            logger.log("Error getting reading \"Sensor\". " + e);
        }
        Long recordedTimestamp = null;
        try {
            recordedTimestamp = doc.getLong("Timestamp");
        } catch (Exception e) {
            logger.log("Error getting reading \"Timestamp\". " + e);
        }
        String readingTimeStr = null;
        try {
            readingTimeStr = doc.getString("Hora");
        } catch (Exception e) {
            logger.log("Error getting reading \"Hora\". " + e);
        }
        if (readingTimeStr != null) {
            readingTimeStr = readingTimeStr.replace(" ", "T");
        }
        LocalDateTime readingTime = null;
        try {
            readingTime = readingTimeStr != null ?
                    LocalDateTime.parse(readingTimeStr) :
                    null;
        } catch (Exception e) {
            logger.log("Could not create java date because \"Hora\" is invalid. " + e);
            e.printStackTrace();
        }

        boolean isOutlier = false;

        if (isExperimentRunning && isValidReading) {
            try {
                isOutlier = isOutlier(doc);
                if (!isOutlier) {
                    checkLimitProximity(doc);
                    limitReached(doc);
                }
                sendTooManyOutliersAlert(doc);
            } catch (SQLException e) {
                if (e.toString().contains("Alerta duplicado")) {
                    logger.log(e.toString());
                } else {
                    logger.log("Error connecting to MariaDB." + e);
                }
            }

            Queue<Double> readingsQueue = sensorReadingsQueues.get(sensor);
            Queue<Double> readingsQueueWithOutliers = sensorReadingsQueuesWithOutliers.get(sensor);

            readingsQueueWithOutliers.offer(reading);
            if (readingsQueueWithOutliers.size() > watcher.getOutlierRecordsNumber()) {
                readingsQueueWithOutliers.poll();
            }
            if (!isOutlier) {
                readingsQueue.offer(reading);
                if (readingsQueue.size() > watcher.getOutlierRecordsNumber()) {
                    readingsQueue.poll();
                }
            }
        }


        Long experimentId = watcher.getIdExperiment();

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

            statement.setInt(6, isValidReading ? 0 : 1);

            statement.setInt(7, isOutlier ? 1 : 0);


            statement.executeUpdate();
            ResultSet generatedKeys = statement.getGeneratedKeys();

            if (generatedKeys.next() && isValidReading && !isOutlier) {
                long newId = generatedKeys.getLong(1);
                try {
                    PreparedStatement procedureStatement = mariadbConnection.prepareStatement(QUERY_SQL_PROCEDURE_TEMP_VARIATION);
                    procedureStatement.setLong(1, newId);
                    procedureStatement.execute();
                } catch (SQLException e) {
                    logger.log(Logger.Severity.WARNING, "Alert not inserted " + e);
                }
            } else {
                throw new SQLException("Creating record failed, no ID obtained.");
            }
            statement.close();
            return true;
        } catch (Exception e) {
            logger.log(Logger.Severity.WARNING, "Error Inserting in the database. " + e);
            e.printStackTrace();
            return false;
        }
    }

    private boolean validateReading(Document doc) {
        boolean validFormat = doc.containsKey("Leitura") &&
                doc.containsKey("Sensor") &&
                doc.containsKey("Hora") &&
                doc.containsKey("Timestamp") &&
                !isWrongTimestamp(doc);
        try {
            doc.getDouble("Leitura");
            doc.getInteger("Sensor");
            doc.getLong("Timestamp");
        } catch (ClassCastException e) {
            return false;
        }
        return validFormat;
    }

    public void checkLimitProximity(Document doc) throws SQLException {

        if (!isExperimentRunning) {
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
        statement.setLong(1, watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, sensor);
        statement.setDouble(4, actualTemp);
        statement.setInt(5, AlertType.AVISO.getValue());

        if (actualTemp <= minLimit && actualTemp >= tempMin) {
            message = String.format(message, "inferior", sensor);
            statement.setString(6, message);
            statement.setInt(7, AlertSubType.TEMPERATURE_NEAR_LIMIT_MIN.getValue());
            statement.executeUpdate();
            // logger.log("Temperatura próxima do limite inferior");
        } else if (actualTemp >= maxLimit && actualTemp <= tempMax) {
            message = String.format(message, "superior", sensor);
            statement.setString(6, message);
            statement.setInt(7, AlertSubType.TEMPERATURE_NEAR_LIMIT_MAX.getValue());
            statement.executeUpdate();
            // logger.log("Temperatura próxima do limite superior");
        }
        statement.close();
    }

    public void limitReached(Document doc) throws SQLException {

        if (!isExperimentRunning) {
            return;
        }

        double actualTemp = doc.getDouble("Leitura");
        int sensor = doc.getInteger("Sensor");

        double tempMin = watcher.getExperimentMinTemp();
        double tempMax = watcher.getExperimentMaxTemp();

        String message = "Temperatura atingiu o limite %s definido no sensor %d.";

        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setLong(1, watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, sensor);
        statement.setDouble(4, actualTemp);
        statement.setInt(5, AlertType.PERIGO.getValue());

        if (actualTemp <= tempMin) {
            message = String.format(message, "mínimo", sensor);
            statement.setString(6, message);
            statement.setInt(7, AlertSubType.TEMPERATURE_LIMIT_REACHED_MIN.getValue());
            statement.executeUpdate();
            // logger.log("Temperatura atingiu limite inferior");
        } else if (actualTemp >= tempMax) {
            message = String.format(message, "máximo", sensor);
            statement.setString(6, message);
            statement.setInt(7, AlertSubType.TEMPERATURE_LIMIT_REACHED_MAX.getValue());
            statement.executeUpdate();
            // logger.log("Temperatura atingiu limite superior");
        }

        statement.close();

    }

    public static void setExperimentRunning(boolean isRunning) {
        synchronized (EXPERIMENT_LOCK) {
            try {
                EXPERIMENT_LOCK.lock();
                isExperimentRunning = isRunning;
            } finally {
                EXPERIMENT_LOCK.unlock();
            }
        }
    }

    public static boolean getExperimentRunning() {
        synchronized (EXPERIMENT_LOCK) {
            try {
                EXPERIMENT_LOCK.lock();
                return isExperimentRunning;
            } finally {
                EXPERIMENT_LOCK.unlock();
            }
        }
    }

    public boolean isOutlier(Document doc) throws SQLException {

        // Get the Sensor
        int sensor = doc.getInteger("Sensor");

        // Actual temperature from mongo record
        double currentReading = doc.getDouble("Leitura");

        // Number of records to get from the db, value defined in each experiment
        int numberOfRecords = watcher.getOutlierRecordsNumber();

        Queue<Double> readingsQueue = sensorReadingsQueues.get(sensor);
        Queue<Double> readingsQueueWithOutliers = sensorReadingsQueuesWithOutliers.get(sensor);

        logger.log("The queue has " + readingsQueue.size() + " records.");

        if (readingsQueue.size() < numberOfRecords) {
            logger.log("Not enough records to start calculating.");
            return false;
        }

        WeightedObservedPoints points = new WeightedObservedPoints();
        int i = 0;
        for (double val : readingsQueue) {
            points.add(i++, val);
        }

        WeightedObservedPoints outlierPoints = new WeightedObservedPoints();
        i = 0;
        for (double val : readingsQueueWithOutliers) {
            outlierPoints.add(i++, val);
        }

        PolynomialCurveFitter fitter = PolynomialCurveFitter.create(3);

        double[] coefficients = fitter.fit(points.toList());
        double[] outlierCoefficients = fitter.fit(outlierPoints.toList());

        PolynomialFunction regression = new PolynomialFunction(coefficients);
        PolynomialFunction outlierRegression = new PolynomialFunction(outlierCoefficients);
        double regressedTemp = regression.value(10);
        double outlierRegressedTemp = outlierRegression.value(10);

        double min = regressedTemp - watcher.getOutlierTempMaxVar();
        double max = regressedTemp + watcher.getOutlierTempMaxVar();

        logger.log("MIN: " + min + " < Average:" + regressedTemp + " < MAX: " + max);
        logger.log("The current reading: " + currentReading);

        boolean isOutlier =
                currentReading > regressedTemp + watcher.getOutlierTempMaxVar() ||
                        currentReading < regressedTemp - watcher.getOutlierTempMaxVar();

        boolean isOutlierWithOutlier =
                currentReading > outlierRegressedTemp + watcher.getOutlierTempMaxVar() ||
                        currentReading < outlierRegressedTemp - watcher.getOutlierTempMaxVar();

        return isOutlier && isOutlierWithOutlier;
    }

    public void sendTooManyOutliersAlert(Document doc) throws SQLException {

        if (!isExperimentRunning) {
            return;
        }

        int sensor = doc.getInteger("Sensor");
        double actualTemp = doc.getDouble("Leitura");

        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_COUNT_OUTLIERS_LAST_X_RECORDS);
        statement.setInt(1, sensor);
        statement.setInt(2, watcher.getOutlierRecordsNumber());
        // logger.log("Number of records " + watcher.getOutlierRecordsNumber());
        ResultSet resultSet = statement.executeQuery();
        int numberOfOutliers = 0;

        if (resultSet.next()) {
            numberOfOutliers = resultSet.getInt("outliers_count");
            // logger.log("Count of outliers " + numberOfOutliers);
        }

        String message = "Sensor de temperatura %d registou demasiados Outliers.";

        try {
            if (numberOfOutliers > MAX_NUMBER_OF_OUTLIERS) {
                statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
                statement.setLong(1, watcher.getIdExperiment());
                statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                statement.setInt(3, sensor);
                statement.setDouble(4, actualTemp);
                statement.setInt(5, AlertType.INFORMATIVO.getValue());
                message = String.format(message, sensor);
                statement.setString(6, message);
                statement.setInt(7, AlertSubType.TEMPERATURE_OUTLIERS.getValue());
                statement.executeUpdate();
                statement.close();
            }
        } catch (SQLException e) {
            logger.log(Logger.Severity.WARNING, e.getMessage() + " Tipo de alerta: " + AlertType.INFORMATIVO.toString());
        }
    }

    public boolean isWrongTimestamp(Document doc) {

        boolean isWrongTimestamp = false;

        // Get the field hour and convert it to LocalDateTime
        String hourFromMongoString = doc.get("Hora").toString();
        LocalDateTime hourFromMongo = null;
        try {
            hourFromMongo = LocalDateTime.parse(hourFromMongoString.replace(" ", "T"));
        } catch (DateTimeParseException e) {

            return false;
        }

        // Get the Timestamp and convert it to LocalDateTime
        Instant instant = Instant.ofEpochMilli(doc.getLong("Timestamp"));
        LocalDateTime timeStampFromMongo = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        // Variable to store 15 earlier than the Timestamp
        LocalDateTime fifteenMinutesEarlier = timeStampFromMongo.minusMinutes(15);

        // If the field Hora from Mongo is after than the timestamp then send alert
        int compareHourWithTimeStamp = hourFromMongo.compareTo(timeStampFromMongo);

        // If the field Hora from Mongo is 15 minutes earlier than the Timestamp then send alert
        int checkLastFifteenMinutes = hourFromMongo.compareTo(fifteenMinutesEarlier);

        if (compareHourWithTimeStamp > 0 || checkLastFifteenMinutes < 0) {
            isWrongTimestamp = true;
        }

        return isWrongTimestamp;
    }

    public void setMariadbConnection(Connection mariadbConnection) {
        this.mariadbConnection = mariadbConnection;
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
                PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
                statement.setLong(1, watcher.getIdExperiment());
                statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                statement.setNull(3, Types.INTEGER);
                statement.setNull(4, Types.DOUBLE);
                statement.setInt(5, AlertType.AVISO.getValue());
                String message = "Sensores de temperatura registaram demasiados erros.";
                statement.setString(6, message);
                statement.setInt(7, AlertSubType.SENSOR_ERRORS.getValue());
                statement.executeUpdate();
            } catch (Exception e) {
                logger.log(Logger.Severity.WARNING, "Error persisting errors alert");
            }


        }
    }

    private TimerTask temperatureReadingsAbsence() {
        return new TimerTask() {
            @Override
            public void run() {
                if (!isExperimentRunning) {
                    return;
                }
                logger.log("Readings Absence (ALERT)");
                try {
                    PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_TEMP_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
                    statement.setLong(1, watcher.getIdExperiment());
                    statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
                    statement.setNull(3, Types.INTEGER);
                    statement.setNull(4, Types.INTEGER);
                    statement.setInt(5, AlertType.PERIGO.getValue());
                    String message = "Não foram recebidas leituras de temperaturas nos últimos 5 segundos.";
                    statement.setString(6, message);
                    statement.setInt(7, AlertSubType.TEMPERATURE_SENSOR_ABSENCE.getValue());
                    statement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    // TODO Ruben - Variação brusca de temperatura


    private void startTimer() {
        temperatureAbsenceTask = temperatureReadingsAbsence();
        timer = new Timer();
        timer.schedule(temperatureAbsenceTask, TEMPS_ABSENCE_LIMIT);
    }

    private void stopTimer() {
        temperatureAbsenceTask.cancel();
        timer.cancel();
        timer.purge();
        timer = null;
        temperatureAbsenceTask = null;
    }

    public static void main(String[] args) {
        Thread.currentThread().setName("Main_Temps_Migration");

        // Immediately launches the Thread: Thread_Experiment_Watcher
        TempsMigrator migrator = new TempsMigrator();
        migrator.run();
    }

}
