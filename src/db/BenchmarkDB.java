package db;

import java.sql.*;

public class BenchmarkDB implements AutoCloseable {
    public static final String USER = "postgres";
    public static final String PASS = "dbidbi";

    private static final String CREATE_BRANCH_SQL =
            "INSERT INTO branches" +
                    "(branchid, branchname, balance, address)" +
                    "VALUES(?, ?, ?, ?)";

    private static final String CREATE_ACCOUNT_SQL =
            "INSERT INTO accounts" +
                    "(accid, name, balance, branchid, address)" +
                    "VALUES(?, ?, ?, ?, ?)";

    private static final String CREATE_TELLER_SQL =
            "INSERT INTO tellers" +
                    "(tellerid, tellername, balance, branchid, address)" +
                    "VALUES(?, ?, ?, ?, ?)";

    private Connection conn;
    private PreparedStatement createBranchStatement;
    private PreparedStatement createAccountStatement;
    private PreparedStatement createTellerStatement;


    public BenchmarkDB(String ip) throws SQLException {
        conn = DriverManager.getConnection("jdbc:postgresql://" + ip + "/ntps", USER, PASS);
    }

    public void createDatabase(int n) throws SQLException {
        for (int i = 1, l = n; i <= l; i++) {
            createBranch(i, i == l);
        }

        for (int i = 1, l = n * 100000; i <= l; i++) {
            int branchId = 1 + (int) (Math.random() * ((n - 1) + 1));
            createAccount(i, branchId, i == l);
        }

        for (int i = 1, l = n * 10; i <= l; i++) {
            int branchId = 1 + (int) (Math.random() * ((n - 1) + 1));
            createTeller(i, branchId, i == l);
        }
    }

    public void createBranch(int id, boolean execute) throws SQLException {
        if (createBranchStatement == null) {
            createBranchStatement = conn.prepareStatement(CREATE_BRANCH_SQL);
        }

        createBranchStatement.setInt(1, id);
        createBranchStatement.setString(2, "aaaaaaaaaaaaaaaaaaaa");
        createBranchStatement.setInt(3, 0);
        createBranchStatement.setString(4, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        createBranchStatement.addBatch();

        if (execute) {
            createBranchStatement.executeBatch();
        }
    }

    public void createAccount(int id, int branchId, boolean execute) throws SQLException {
        if (createAccountStatement == null) {
            createAccountStatement = conn.prepareStatement(CREATE_ACCOUNT_SQL);
        }

        createAccountStatement.setInt(1, id);
        createBranchStatement.setString(2, "aaaaaaaaaaaaaaaaaaaa");
        createBranchStatement.setInt(3, 0);
        createAccountStatement.setInt(4, branchId);
        createBranchStatement.setString(5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        createAccountStatement.addBatch();

        if (execute) {
            createAccountStatement.executeBatch();
        }
    }

    public void createTeller(int id, int branchId, boolean execute) throws SQLException {
        if (createTellerStatement == null) {
            createTellerStatement = conn.prepareStatement(CREATE_TELLER_SQL);
        }

        createTellerStatement.setInt(1, id);
        createBranchStatement.setString(2, "aaaaaaaaaaaaaaaaaaaa");
        createBranchStatement.setInt(3, 0);
        createTellerStatement.setInt(4, branchId);
        createBranchStatement.setString(5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        createTellerStatement.addBatch();

        if (execute) {
            createTellerStatement.executeBatch();
        }
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }
}

