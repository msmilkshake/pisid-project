package com.tailwaglabs.core;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.migrator.MovsMigrator;
import com.tailwaglabs.core.migrator.TempsMigrator;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This thread is responsible for detecting when an experiment starts.
 * It's also responsible to check if an experiment exceeded 10 mins
 * <p>
 * The TempsMNigrator class runs this thread when it starts.
 */
public class ExperimentWatcher extends Thread {

    private final boolean LOGGER_ENABLED = true;

    private static ExperimentWatcher watcher = new ExperimentWatcher();
    private static TempsMigrator migrator = null;

    static {
        watcher.start();
    }

    public static ExperimentWatcher getInstance() {
        return watcher;
    }

    // ini MariaDB Properties
    private String sqlConnection = "";
    private String sqlusername = "";
    private String sqlPassword = "";

    private MongoClient mongoClient;
    private MongoDatabase db;

    private Connection mariadbConnection;

    private final int REFRESH_RATE = 1 * 1000; // 1 second
    private final int EXPERIMENT_MAX_DURATION = 10 * 60 * 1000; //MIN SEC MSEC

    Logger logger = null;

    private Long idExperiment = null;

    private double experimentMinTemp;
    private double experimentMaxTemp;
    private double outlierTempMaxVar;
    private int outlierRecordsNumber;
    private int miceLimit;
    private int startingMiceNumber;
    private int secondsWithoutMovement;

    private final String QUERY_GET_EXPERIENCE_PARAMETERS = """ 
            SELECT parametrosexperiencia.TemperaturaMaxima, parametrosexperiencia.TemperaturaMinima,
            parametrosexperiencia.OutlierVariacaoTempMax, parametrosexperiencia.OutlierLeiturasNumero,
            parametrosexperiencia.LimiteRatosSala, parametrosexperiencia.NumeroRatosInicial,
            parametrosexperiencia.SegundosSemMovimento
            FROM experiencia
            JOIN parametrosexperiencia ON experiencia.IDParametros = parametrosexperiencia.IDParametros
            WHERE experiencia.IDExperiencia = ?;
            """;

    public final String QUERY_SQL_GET_RUNNING_EXPERIMENT = """
            SELECT IDExperiencia
            FROM experiencia
            WHERE IDEstado = 2;
            """;

    private final String QUERY_SQL_INSERT_ALERT = """ 
            INSERT INTO alerta(IDExperiencia, Hora, TipoAlerta, Mensagem, SubTipoAlerta)
            VALUES (?, ?, ?, ?, ?)
            """;

    Timer timer = null;
    TimerTask experimentLimitTask = null;

    public static void setMigratorInstance(TempsMigrator migrator) {
        ExperimentWatcher.migrator = migrator;
    }

    private TimerTask createExperimentLimitTask() {
        return new TimerTask() {
            @Override
            public void run() {
                try {
                    experimentRunningLimit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }


    private void init() {
        logger = new Logger("Thread_Experiment_Watcher", Logger.TextColor.PURPLE);
        logger.setEnabled(LOGGER_ENABLED);
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("config.ini"));

            sqlConnection = p.getProperty("sql_database_connection_to");
            sqlPassword = p.getProperty("sql_database_password_to");
            sqlusername = p.getProperty("sql_database_user_to");

            mariadbConnection = DriverManager.getConnection(sqlConnection, sqlusername, sqlPassword);
        } catch (SQLException e) {
            logger.log("Error connecting to MariaDB." + e);
        } catch (IOException e) {
            logger.log("Error reading config.ini file." + e);
        }
    }


    @Override
    public void run() {
        Thread.currentThread().setName("Thread_Experiment_Watcher");
        init();
        watchLoop();
    }

    public void watchLoop() {
        while (true) {
            try {
                ResultSet experimentResults = mariadbConnection.createStatement()
                        .executeQuery(QUERY_SQL_GET_RUNNING_EXPERIMENT);

                if (experimentResults.isBeforeFirst() && idExperiment == null) {
                    startExperiment(experimentResults);
                } else if (!experimentResults.isBeforeFirst() && idExperiment != null) {
                    stopExperiment();
                }

//                logger.log("Sleeping " + (REFRESH_RATE / 1000) + " seconds."); // REINSTATE
                Thread.sleep(REFRESH_RATE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startExperiment(ResultSet results) throws SQLException {
        results.next();
        idExperiment = results.getLong(1);
        setExperimentParameters();
        TempsMigrator.setExperimentRunning(true);
        experimentLimitTask = createExperimentLimitTask();
        startTimer();
        migrator.restartQueues();
        new MovsMigrator().start();
        logger.log("Experiment #" + idExperiment + " started.");
    }

    private void stopExperiment() {
        TempsMigrator.setExperimentRunning(false);
        stopTimer();
        logger.log("Experiment #" + idExperiment + " ended.");
        idExperiment = null;
    }

    public Long getIdExperiment() {
        return idExperiment;
    }

    public void setExperimentParameters() throws SQLException {
        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_GET_EXPERIENCE_PARAMETERS);
        statement.setLong(1, idExperiment);
        ResultSet resultSet = statement.executeQuery();

        if (resultSet.next()) {
            experimentMinTemp = resultSet.getDouble("TemperaturaMinima");
            experimentMaxTemp = resultSet.getDouble("TemperaturaMaxima");
            outlierTempMaxVar = resultSet.getDouble("OutlierVariacaoTempMax");
            outlierRecordsNumber = resultSet.getInt("OutlierLeiturasNumero");
            miceLimit = resultSet.getInt("LimiteRatosSala");
            startingMiceNumber = resultSet.getInt("NumeroRatosInicial");
            secondsWithoutMovement = resultSet.getInt("SegundosSemMovimento");
        }
    }

    public double getExperimentMaxTemp() {
        return experimentMaxTemp;
    }

    public double getExperimentMinTemp() {
        return experimentMinTemp;
    }

    public double getOutlierTempMaxVar() {
        return outlierTempMaxVar;
    }

    public int getOutlierRecordsNumber() {
        return outlierRecordsNumber;
    }

    public int getMiceLimit() {
        return miceLimit;
    }

    public int getStartingMiceNumber() {
        return startingMiceNumber;
    }

    public int getSecondsWithoutMovement() {
        return secondsWithoutMovement;
    }

    // Call this when an experiment starts
    private void startTimer() {
        timer = new Timer();
        timer.schedule(experimentLimitTask, EXPERIMENT_MAX_DURATION);
    }

    // Call this when an experimnent ends
    private void stopTimer() {
        experimentLimitTask.cancel();
        timer.cancel();
        timer.purge();
        timer = null;
        experimentLimitTask = null;
    }

    public void experimentRunningLimit() throws SQLException {

        String message = "Experiência a decorrer há mais de 10 minutos.";
        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setLong(1, watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, AlertType.INFORMATIVO.getValue());
        statement.setString(4, message);
        statement.setInt(5, AlertSubType.EXPERIMENT_DURATION.getValue());
        statement.executeUpdate();
        statement.close();
        logger.log("ALERT - EXPERIMENT RUNNING FOR 10 MINUTES!");
    }

    public void alertMovementAbsence(long segundos) throws SQLException {
        String message = String.format("Ratos parados há mais de %d segundos.", segundos);
        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_INSERT_ALERT, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setLong(1, watcher.getIdExperiment());
        statement.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
        statement.setInt(3, AlertType.INFORMATIVO.getValue());
        statement.setString(4, message);
        statement.setInt(5, AlertSubType.MICE_MOVEMENT_ABSENCE.getValue());
        statement.executeUpdate();
        statement.close();
        System.out.printf("ALERT - MICE STOPPED FOR %d SECONDS!\n", segundos);
    }
}
