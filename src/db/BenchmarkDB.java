package db;

import java.sql.*;

public class BenchmarkDB implements AutoCloseable {
    public static final String URL = "jdbc:postgresql://192.168.0.215/ntps";
    public static final String USER = "dbi";
    public static final String PASS = "dbidbi";

    private static final String CREATE_BRANCH_SQL =
            "INSERT INTO branches" +
            "(branchid, branchname, balance, address)" +
            "VALUES(?, 'aaaaaaaaaaaaaaaaaaaa', 0, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')";

    private static final String CREATE_ACCOUNT_SQL =
            "INSERT INTO accounts" +
            "(accid, name, balance, branchid, address)" +
            "VALUES(?, 'aaaaaaaaaaaaaaaaaaaa', 0, ?, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')";

    private static final String CREATE_TELLER_SQL =
            "INSERT INTO tellers" +
            "(tellerid, tellername, balance, branchid, address)" +
            "VALUES(?, 'aaaaaaaaaaaaaaaaaaaa', 0, ?, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')";

    private Connection conn;
    private PreparedStatement createBranchStatement;
    private PreparedStatement createAccountStatement;
    private PreparedStatement createTellerStatement;


    public BenchmarkDB() throws SQLException {
        conn = DriverManager.getConnection(URL, USER, PASS);
    }

    public void createDatabase(int n) {
        try {
            for (int i = 1; i <= n; i++) {
                createBranch(i, i == n);
            }

            for (int i = 1, l = n * 100000; i <= l; i++) {
                int branchId = 1 + (int)(Math.random() * ((n - 1) + 1));
                createAccount(i, branchId, i == l);
            }

            for (int i = 1, l = n * 10; i <= l; i++) {
                int branchId = 1 + (int)(Math.random() * ((n - 1) + 1));
                createTeller(i, branchId, i == l);
            }
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void createBranch(int id, boolean commit) throws SQLException {
        if (createBranchStatement == null) {
            createBranchStatement = conn.prepareStatement(CREATE_BRANCH_SQL);
        }

        createBranchStatement.setInt(1, id);
        createBranchStatement.addBatch();

        if (commit) {
            createBranchStatement.executeLargeBatch();
        }
    }

    public void createAccount(int id, int branchId, boolean commit) throws SQLException {
        if (createAccountStatement == null) {
            createAccountStatement = conn.prepareStatement(CREATE_ACCOUNT_SQL);
        }

        createAccountStatement.setInt(1, id);
        createAccountStatement.setInt(2, branchId);
        createAccountStatement.addBatch();

        if (commit) {
            createAccountStatement.executeLargeBatch();
        }
    }

    public void createTeller(int id, int branchId, boolean commit) throws SQLException {
        if (createTellerStatement == null) {
            createTellerStatement = conn.prepareStatement(CREATE_TELLER_SQL);
        }

        createTellerStatement.setInt(1, id);
        createTellerStatement.setInt(2, branchId);
        createTellerStatement.addBatch();

        if (commit) {
            createTellerStatement.executeLargeBatch();
        }
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }
}

