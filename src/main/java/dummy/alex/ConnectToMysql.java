package dummy.alex;

import java.sql.*;

public class ConnectToMysql {
    private static void show_matrix(int[][] m) {
        System.out.println("    1234567890");
        for (int i = 1; i < m.length; i++) { // array idx 0 not displayed/used
            System.out.printf("%2d  ", i);
            for (int j = 1; j < m.length; j++) { // array idx 0 not displayed/used
                System.out.print(m[i][j]);
            }
            System.out.println();
        }
    }

    private static int get_number_of_rooms(Connection connection) throws SQLException { // Query DB to find number of rooms / temperature
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM configuracaolabirinto");
        resultSet.next();
//        double temperature = resultSet.getDouble("temperaturaprogramada");  // verificar se será necessário
        return resultSet.getInt("numerodesalas") + 1;
    }

    private static void load_topology(Connection connection, int[][] topology) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM corredor");
        while (resultSet.next()) {
            int A = resultSet.getInt("salaa");
            int B = resultSet.getInt("salab");
//            int cent = resultSet.getInt("centimetro");  // verificar se será necessário
//            System.out.println("Sala " + A + " -> " + B + " - Dist: " + cent + " cm"); // visualize data retrieved
            topology[A][B] = 1;  // load adjacency matrix
        }
        resultSet.close();
    }

    public static void main(String[] args) {
        String url = "jdbc:mysql://194.210.86.10/pisid2024";
        String username = "aluno";
        String password = "aluno";

        try {
            // Load the MySQL JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Get a connection to the database
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println("Connected to MySQL database!\n");
            int nbRooms = get_number_of_rooms(connection);
            // create adjacency matrix nbRooms wide
            int[][] topology = new int[nbRooms][nbRooms];
            // Query DB to find labyrinth topology
            load_topology(connection, topology);
            show_matrix(topology);
            connection.close();

        } catch (ClassNotFoundException e) {
            System.out.println("Error: MySQL JDBC driver not found!");
            e.printStackTrace();

        } catch (SQLException e) {
            System.out.println("Error: Could not connect to the database!");
            e.printStackTrace();
        }
    }
}
