package db;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 */
public class BenchmarkDB implements AutoCloseable {
    public static final String USER = "postgres";
    public static final String PASS = "dbidbi";

    private Connection conn;

    /**
     * @param ip
     * @throws SQLException
     */
    public BenchmarkDB(String ip) throws SQLException {
        conn = DriverManager.getConnection("jdbc:postgresql://" + ip + "/ntps", USER, PASS);
    }

    /**
     * @throws SQLException
     */
    public void clearDatabase() throws SQLException {
        conn.createStatement()
                .execute("TRUNCATE accounts CASCADE;\n" +
                        "TRUNCATE tellers CASCADE;\n" +
                        "TRUNCATE branches CASCADE;");
    }

    /**
     * @param n
     * @throws InvalidParameterException
     * @throws SQLException
     * @throws InterruptedException
     */
    public void createDatabase(int n) throws InvalidParameterException, SQLException, InterruptedException {
        if ((0 < n && n < 5) || n % 5 != 0) {
            throw new InvalidParameterException("Parameter violation\nn must be 0 < n < 5 || n % 5 == 0");
        }

        setTableLog(false);
        removeKeyConstraints();

        Thread branchThread = new Thread(() -> createBranches(n));
        branchThread.start();

        Thread tellerThread = new Thread(() -> createTellers(n));
        tellerThread.start();

        Thread[] accountThreads;
        int threadFactor = n < 5 ? n : 5;

        accountThreads = new Thread[n / threadFactor];
        for (int i = 0; i < n / 5; i++) {
            final int from = i * 100000 * threadFactor + 1;
            final int to = (i + 1) * 100000 * threadFactor;
            accountThreads[i] = new Thread(() -> createAccounts(n, from, to));
            accountThreads[i].start();
        }

        branchThread.join();
        tellerThread.join();
        for (int i = 0; i < n / threadFactor; i++) {
            accountThreads[i].join();
        }

        addKeyConstraints();
        setTableLog(true);
    }

    /**
     * @throws SQLException
     */
    private void addKeyConstraints() throws SQLException {
        conn.createStatement()
                .execute("ALTER TABLE accounts ADD PRIMARY KEY (accid);\n" +
                        "ALTER TABLE branches ADD PRIMARY KEY (branchid);\n" +
                        "ALTER TABLE tellers ADD PRIMARY KEY (tellerid);\n" +
                        "ALTER TABLE accounts ADD FOREIGN KEY (branchid) REFERENCES branches;\n" +
                        "ALTER TABLE tellers ADD FOREIGN KEY (branchid) REFERENCES branches;\n" +
                        "ALTER TABLE history ADD FOREIGN KEY (accid) REFERENCES accounts,\n" +
                        "  ADD FOREIGN KEY (branchid) REFERENCES branches,\n" +
                        "  ADD FOREIGN KEY (tellerid) REFERENCES tellers;");
    }

    /**
     * @throws SQLException
     */
    private void removeKeyConstraints() throws SQLException {
        conn.createStatement()
                .execute("ALTER TABLE accounts DROP CONSTRAINT accounts_pkey CASCADE;\n" +
                        "ALTER TABLE branches DROP CONSTRAINT branches_pkey CASCADE;\n" +
                        "ALTER TABLE tellers DROP CONSTRAINT tellers_pkey CASCADE;");
    }

    /**
     * @param logged
     * @throws SQLException
     */
    public void setTableLog(boolean logged) throws SQLException {
        String sql = logged ? (
                "ALTER TABLE branches SET LOGGED; " +
                        "ALTER TABLE tellers SET LOGGED; " +
                        "ALTER TABLE accounts SET LOGGED; " +
                        "ALTER TABLE history SET LOGGED;"
        ) : (
                "ALTER TABLE history SET UNLOGGED; " +
                        "ALTER TABLE tellers SET UNLOGGED; " +
                        "ALTER TABLE accounts SET UNLOGGED; " +
                        "ALTER TABLE branches SET UNLOGGED;"
        );

        Statement statement = conn.createStatement();
        statement.execute(sql);
    }

    /**
     * @param n
     */
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
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     #* @param n
     * @param from
     * @param to
     */
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
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * @param n
     */
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
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException {
        conn.close();
    }
}

