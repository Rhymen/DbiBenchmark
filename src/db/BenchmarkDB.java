package db;

import java.sql.*;

public class BenchmarkDB implements AutoCloseable {
    public static final String URL = "jdbc:postgresql://192.168.0.215/ntps";
    public static final String USER = "dbi";
    public static final String PASS = "dbidbi";

    private Connection conn;

    public BenchmarkDB() throws SQLException {
        conn = DriverManager.getConnection(URL, USER, PASS);
    }

    public void createDatabase(int n) {
        try {
            createBranch(n);
            createAccount(n*100000, n);
            createTeller(n*10, n);
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void createBranch(int id) throws SQLException {
        final Statement stmt = conn.createStatement();

        for(int i = 1; i <= id; i++){
            final String sql = "" +
                    "INSERT INTO branches" +
                    "(branchid, branchname, balance, address)" +
                    "VALUES(" + i + ", 'aaaaaaaaaaaaaaaaaaaa', 0, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')";

            stmt.addBatch(sql);
        }
        stmt.executeBatch();
    }

    public void createAccount(int id, int n) throws SQLException {
        final Statement stmt = conn.createStatement();
        for(int i = 1; i < id; i++){
            int branchId = (int) (Math.random() * ((n - 1) + 1));
            final String sql = "" +
                    "INSERT INTO accounts" +
                    "(accid, name, balance, branchid, address)" +
                    "VALUES(" + id + ", 'aaaaaaaaaaaaaaaaaaaa', 0, " + branchId + ", 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')";
            stmt.addBatch(sql);

            if(i%1000 == 0){
                stmt.executeBatch();
            }
        }
        stmt.executeBatch();
    }

    public void createTeller(int id, int n) throws SQLException {
        final Statement stmt = conn.createStatement();
        for(int i = 1; i < id; i++){
            int branchId = (int) (Math.random() * ((n - 1) + 1));
            final String sql = "" +
                    "INSERT INTO tellers" +
                    "(tellerid, tellername, balance, branchid, address)" +
                    "VALUES(" + id + ", 'aaaaaaaaaaaaaaaaaaaa', 0, " + branchId + ", 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')";
            stmt.addBatch(sql);
        }

        stmt.executeBatch();
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }
}

