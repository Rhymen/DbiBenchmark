package db;

import java.sql.*;

public class BenchmarkDB implements AutoCloseable {
    public static final String URL = "jdbc:postgresql://192.168.0.215/CAP";
    public static final String USER = "postgres";
    public static final String PASS = "dbidbi";

    private Connection conn;

    public BenchmarkDB() throws SQLException {
        conn = DriverManager.getConnection(URL, USER, PASS);
    }

    public void createDatabase(int n) {
        try {
            for (int i = 1; i <= n; i++) {
                createBranch(i);
            }

            for (int i = 1, l = n * 10000; i <= l; i++) {
                int branchId = 1 + (int)(Math.random() * ((n - 1) + 1));
                createAccount(i, branchId);
            }

            for (int i = 1, l = n * 10; i <= l; i++) {
                int branchId = 1 + (int)(Math.random() * ((n - 1) + 1));
                createAccount(i, branchId);
            }
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void createBranch(int id) throws SQLException {
        final String sql = "" +
                "INSERT INTO branches" +
                "(branchid, branchname, balance, address)" +
                "VALUES(" + id + ", aaaaaaaaaaaaaaaaaaaa, 0, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)";

        final Statement stmt = conn.createStatement();
        stmt.executeQuery(sql);
    }

    public void createAccount(int id, int branchId) throws SQLException {
        final String sql = "" +
                "INSERT INTO accounts" +
                "(accid, name, balance, branchid, address)" +
                "VALUES(" + id + ", aaaaaaaaaaaaaaaaaaaa, 0, " + branchId + ", aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)";

        final Statement stmt = conn.createStatement();
        stmt.executeQuery(sql);
    }

    public void createTeller(int id, int branchId) throws SQLException {
        final String sql = "" +
                "INSERT INTO tellers" +
                "(tellerid, tellername, balance, branchid, address)" +
                "VALUES(" + id + ", aaaaaaaaaaaaaaaaaaaa, 0, " + branchId + ", aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa)";

        final Statement stmt = conn.createStatement();
        stmt.executeQuery(sql);
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }
}

