package db;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BenchmarkDB implements AutoCloseable {
    public static final String USER = "postgres";
    public static final String PASS = "dbidbi";

    private Connection conn;


    public BenchmarkDB(String ip) throws SQLException {
        conn = DriverManager.getConnection("jdbc:postgresql://" + ip + "/ntps", USER, PASS);
    }

    public void createDatabase(int n) throws SQLException, InterruptedException {
        setTableLog(false);

        Thread branchThread = new Thread(() -> createBranches(n));
        Thread accountsThread = new Thread(() -> createAccounts(n, 1, n * 50000));
        Thread accountsThread2 = new Thread(() -> createAccounts(n, n * 50000 + 1, n * 100000));
        Thread tellerThread = new Thread(() -> createTellers(n));

        branchThread.start();
        accountsThread.start();
        accountsThread2.start();
        tellerThread.start();

        branchThread.join();
        accountsThread.join();
        accountsThread2.join();
        tellerThread.join();

        setTableLog(true);
    }

    public void setTableLog(boolean logged) {
        try {
            Statement statement = conn.createStatement();
            if (logged) {
                statement.execute("ALTER TABLE branches SET LOGGED; " +
                        "ALTER TABLE tellers SET LOGGED; " +
                        "ALTER TABLE accounts SET LOGGED; " +
                        "ALTER TABLE history SET LOGGED;");
            } else {
                statement.execute("ALTER TABLE history SET UNLOGGED; " +
                        "ALTER TABLE tellers SET UNLOGGED; " +
                        "ALTER TABLE accounts SET UNLOGGED; " +
                        "ALTER TABLE branches SET UNLOGGED;");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createBranches(int n) {
        try {
            CopyManager copyAPI = ((PGConnection) conn).getCopyAPI();
            CopyIn in = copyAPI.copyIn("COPY branches FROM STDIN WITH DELIMITER ','");
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= n; i++) {
                sb.setLength(0);
                sb.append(i)
                        .append(',')
                        .append("aaaaaaaaaaaaaaaaaaaa")
                        .append(',')
                        .append(0)
                        .append(',')
                        .append("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                        .append('\n');
                in.writeToCopy(sb.toString().getBytes(), 0, sb.length());
            }

            in.endCopy();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createAccounts(int n, int from, int to) {
        try {
            CopyManager copyAPI = ((PGConnection) conn).getCopyAPI();
            CopyIn in = copyAPI.copyIn("COPY accounts FROM STDIN WITH DELIMITER ','");
            StringBuilder sb = new StringBuilder();
            for (int i = from, l = to; i <= l; i++) {
                sb.setLength(0);
                sb.append(i)
                        .append(',')
                        .append("aaaaaaaaaaaaaaaaaaaa")
                        .append(',')
                        .append(0)
                        .append(',')
                        .append(1 + (int) (Math.random() * ((n - 1) + 1)))
                        .append(',')
                        .append("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                        .append('\n');
                in.writeToCopy(sb.toString().getBytes(), 0, sb.length());
            }

            in.endCopy();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createTellers(int n) {
        try {
            CopyManager copyAPI = ((PGConnection) conn).getCopyAPI();
            CopyIn in = copyAPI.copyIn("COPY tellers FROM STDIN WITH DELIMITER ','");
            StringBuilder sb = new StringBuilder();
            for (int i = 1, l = n * 10; i <= l; i++) {
                sb.setLength(0);
                sb.append(i)
                        .append(',')
                        .append("aaaaaaaaaaaaaaaaaaaa")
                        .append(',')
                        .append(0)
                        .append(',')
                        .append(1 + (int) (Math.random() * ((n - 1) + 1)))
                        .append(',')
                        .append("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                        .append('\n');
                in.writeToCopy(sb.toString().getBytes(), 0, sb.length());
            }

            in.endCopy();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }
}

