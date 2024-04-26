package com.tailwaglabs.core;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.tailwaglabs.core.migrator.TempsMigrator;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
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


    Timer timer = null;

    private final int REFRESH_RATE = 1 * 1000; // 1 second
    private final int EXPERIMENT_MAX_DURATION = 1 * 15 * 1000;

    private Long idExperiment = null;

    private double experimentMinTemp;
    private double experimentMaxTemp;
    private double outlierTempMaxVar;
    private int outlierRecordsNumber;

    private final String QUERY_SQL_GET_TEMP_MIN_MAX = """ 
            SELECT parametrosexperiencia.TemperaturaMaxima, parametrosexperiencia.TemperaturaMinima,
            parametrosexperiencia.OutlierVariacaoTempMax, parametrosexperiencia.OutlierLeiturasNumero
            FROM experiencia
            JOIN parametrosexperiencia ON experiencia.IDParametros = parametrosexperiencia.IDParametros
            WHERE experiencia.IDExperiencia = ?;
            """;

    public final String QUERY_SQL_GET_RUNNING_EXPERIMENT = """
            SELECT IDExperiencia
            FROM experiencia
            WHERE IDEstado = 2;
            """;

    TimerTask experimentLimitTask = null;

    public static void setMigratorInstance(TempsMigrator migrator) {
        ExperimentWatcher.migrator = migrator;
    }

    private TimerTask createExperimentLimitTask() {
        return new TimerTask() {
            @Override
            public void run() {
                myMethod();
            }
        };
    }


    private void init() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("config.ini"));

            sqlConnection = p.getProperty("sql_database_connection_to");
            sqlPassword = p.getProperty("sql_database_password_to");
            sqlusername = p.getProperty("sql_database_user_to");

            mariadbConnection = DriverManager.getConnection(sqlConnection, sqlusername, sqlPassword);
        } catch (SQLException e) {
            System.out.println("Error connecting to MariaDB." + e);
        } catch (IOException e) {
            System.out.println("Error reading config.ini file." + e);
        }

    }


    @Override
    public void run() {
        Thread.currentThread().setName("Thread_Experiment_Watcher");
        init();
        watchLoop();
        try {
            setExperimentParameters();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void watchLoop() {
        while (true) {
            try {
                ResultSet experimentResults = mariadbConnection.createStatement()
                        .executeQuery(QUERY_SQL_GET_RUNNING_EXPERIMENT);

                if (experimentResults.isBeforeFirst() && idExperiment == null) {
                    experimentResults.next();
                    idExperiment = experimentResults.getLong(1);
                    setExperimentParameters();
                    TempsMigrator.setExperimentRunning(true);
                    experimentLimitTask = createExperimentLimitTask();
                    startTimer();
                    migrator.restartQueues();
                    System.out.println("[" + Thread.currentThread().getName() + "] Experiment #" + idExperiment + " started.");
                } else if (!experimentResults.isBeforeFirst() && idExperiment != null) {
                    TempsMigrator.setExperimentRunning(false);
                    stopTimer();
                    System.out.println("[" + Thread.currentThread().getName() + "] Experiment #" + idExperiment + " ended.");
                    idExperiment = null;
                }


                System.out.println("[" + Thread.currentThread().getName() + "] Sleeping " + (REFRESH_RATE / 1000) + " seconds.");
                Thread.sleep(REFRESH_RATE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Long getIdExperiment() {
        return idExperiment;
    }

    public void setExperimentParameters() throws SQLException {
        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_GET_TEMP_MIN_MAX);
        statement.setLong(1, idExperiment);
        ResultSet resultSet = statement.executeQuery();

        if (resultSet.next()) {
            experimentMinTemp = resultSet.getDouble("TemperaturaMinima");

            experimentMaxTemp = resultSet.getDouble("TemperaturaMaxima");

            outlierTempMaxVar = resultSet.getDouble("OutlierVariacaoTempMax");
            outlierRecordsNumber = resultSet.getInt("OutlierLeiturasNumero");

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

    public void myMethod() {
        // TODO: This method runs after 10 minutes
        // ALERT Experiment duration
        System.out.println("ALERT - EXPERIMENT RUNNING FOR 15 SECONDS!!");
    }
}
