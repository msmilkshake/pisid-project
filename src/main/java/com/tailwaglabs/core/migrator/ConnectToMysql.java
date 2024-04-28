package com.tailwaglabs.core.migrator;
import com.tailwaglabs.core.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class ConnectToMysql {
    private Connection bd_cloud_connection;
    private String bd_cloud_con = "";
    private String bd_cloud_user = "";
    private String bd_cloud_pass = "";
    Logger logger = null;

    public static void show_matrix(int[][] m) {
        System.out.println("Labyrinth topology:");
        System.out.println("    1234567890");
        for (int i = 1; i < m.length; i++) { // array idx 0 not displayed/used
            System.out.printf("%2d  ", i);
            for (int j = 1; j < m.length; j++) { // array idx 0 not displayed/used
                System.out.print(m[i][j]);
            }
            System.out.println();
        }
    }

    private int get_number_of_rooms(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT numerodesalas FROM configuracaolabirinto");
        resultSet.next();
        int nbRooms = resultSet.getInt("numerodesalas") + 1;
        resultSet.close();
        return nbRooms;
    }

    private void load_topology(Connection connection, int[][] topology) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT salaa, salab FROM corredor");

        while (resultSet.next()) {
            int a = resultSet.getInt("salaa");
            int b = resultSet.getInt("salab");
            topology[a][b] = 1;  // load adjacency matrix
        }
        resultSet.close();
    }

    public int[][] getTopology() {
        Thread.currentThread().setName("Topology getter");
        logger = new Logger("Main_Temps_Migration", Logger.TextColor.YELLOW);
        int[][] topology = null;

        try {
            Properties p = new Properties();
            p.load(new FileInputStream("config.ini"));
            bd_cloud_con = p.getProperty("bd_cloud_connection");
            bd_cloud_user = p.getProperty("bd_cloud_user");
            bd_cloud_pass = p.getProperty("bd_cloud_password");
            bd_cloud_connection = DriverManager.getConnection(bd_cloud_con, bd_cloud_pass, bd_cloud_user);
            int nbRooms = get_number_of_rooms(bd_cloud_connection);
            // create adjacency matrix nbRooms wide
            topology = new int[nbRooms][nbRooms];
            // Query DB to find labyrinth topology
            load_topology(bd_cloud_connection, topology);
            bd_cloud_connection.close();
        } catch (SQLException e) {
            logger.log("Error: Could not connect to the database! " + e);
        } catch (IOException e) {
            logger.log("Error reading config.ini file." + e);
        }
        return topology;
    }
}
