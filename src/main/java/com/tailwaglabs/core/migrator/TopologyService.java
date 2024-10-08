package com.tailwaglabs.core.migrator;
import com.tailwaglabs.core.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class TopologyService {
    private Connection bd_cloud_connection;
    private String bd_cloud_con = "";
    private String bd_cloud_user = "";
    private String bd_cloud_pass = "";
    private Logger logger = null;

    public void show_matrix(int[][] m) {
        logger.log("Labyrinth topology:");
        logger.log("    1234567890");
        for (int i = 1; i < m.length; i++) { // array idx 0 not displayed/used
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%2d  ", i));
            for (int j = 1; j < m.length; j++) { // array idx 0 not displayed/used
                sb.append(m[i][j]);
            }
            logger.log(sb);
        }
    }

    private int get_number_of_rooms(Connection connection) throws SQLException {  // obsolete after teacher email
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
            int a = resultSet.getInt("salaa");  // index of ROW in two-dimension array (1 - 10)
            int b = resultSet.getInt("salab");  // index of COL in two-dimension array (1 - 10)
            try {
                topology[a][b] = 1;  // load adjacency matrix -> 1 represents connection between rooms (default is 0)
            } catch (ArrayIndexOutOfBoundsException e) { // deals with anomalous changes in cloud db (obsolete)
//                logger.log("Load topology: " + e);
            }
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
//            int nbRooms = get_number_of_rooms(bd_cloud_connection); // obsolete
            // create adjacency matrix nbRooms wide
            topology = new int[11][11]; // array size hard coded to support up to 10 rooms (1 - 10)
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
