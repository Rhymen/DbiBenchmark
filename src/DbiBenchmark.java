import db.BenchmarkDB;

import java.time.Duration;
import java.time.Instant;

public class DbiBenchmark {
    public static void main(String[] args) {
        String ip = args[0];
        int n = Integer.parseInt(args[1]);

        try (BenchmarkDB db = new BenchmarkDB(ip)) {
            db.clearDatabase();
            
            Instant starts = Instant.now();
            db.createDatabase(n);
            Instant ends = Instant.now();
            System.out.println(Duration.between(starts, ends));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
