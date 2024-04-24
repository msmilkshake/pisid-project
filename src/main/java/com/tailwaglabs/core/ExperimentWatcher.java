package com.tailwaglabs.core;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

/**
 * This thread is responsible for detecting when an experiment starts.
 * It's also responsible to check if an experiment exceeded 10 mins
 *
 * The TempsMNigrator class runs this thread when it starts.
 */
public class ExperimentWatcher extends Thread {

    private static ExperimentWatcher watcher = new ExperimentWatcher();

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


    Timer timer = new Timer();

    private int experimentDuration = 10 * 60 * 1000;

    private int idExperiment = 1;

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

    TimerTask task = new TimerTask() {
        @Override
        public void run() {
            myMethod();
        }
    };


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
        try {
            setExperimentParameters();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int getIdExperiment() {
        return idExperiment;
    }

    public void setExperimentParameters() throws SQLException {
        PreparedStatement statement = mariadbConnection.prepareStatement(QUERY_SQL_GET_TEMP_MIN_MAX);
        statement.setInt(1, idExperiment);
        ResultSet resultSet = statement.executeQuery();

        if(resultSet.next()) {
            experimentMinTemp = resultSet.getDouble("TemperaturaMinima");;
            experimentMaxTemp = resultSet.getDouble("TemperaturaMaxima");;
            outlierTempMaxVar = resultSet.getDouble("OutlierVariacaoTempMax");
            outlierRecordsNumber = resultSet.getInt("OutlierLeiturasNumero");;
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
        timer.schedule(task, experimentDuration);
    }

    // Call this when an experimnent ends
    private void stopTimer() {
        task.cancel();
        timer.cancel();
        timer.purge();
    }

    public void myMethod() {
        // TODO: Tis method runs after 10 minutes
        // ALERT Experiment duration
    }
}
