package com.tailwaglabs.core.migrator;

import java.sql.*;

public class ConnectToMysql {
    public static void show_matrix(int[][] m) {
        System.out.println("Labyrinth topology");
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
//            System.out.println(STR."Sala \{a} -> \{b}"); // visualize data retrieved REMOVE
            topology[a][b] = 1;  // load adjacency matrix
        }
        resultSet.close();
    }

    public int[][] getTopology() {
        String url = "jdbc:mysql://194.210.86.10/pisid2024";
        String username = "aluno";
        String password = "aluno";
        int[][] topology = null;

        try {
            // Load the MySQL JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Get a connection to the database
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connected to MySQL database!\n");
            int nbRooms = get_number_of_rooms(connection);
            // create adjacency matrix nbRooms wide
            topology = new int[nbRooms][nbRooms];
            // Query DB to find labyrinth topology
            load_topology(connection, topology);
//            show_matrix(topology); // TO REMOVE
            connection.close();
        } catch (ClassNotFoundException e) {
            System.out.println("Error: MySQL JDBC driver not found!");
            e.printStackTrace();

        } catch (SQLException e) {
            System.out.println("Error: Could not connect to the database!");
            e.printStackTrace();
        }
        return topology;
    }
}
