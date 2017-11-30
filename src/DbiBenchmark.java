import db.BenchmarkDB;

import java.time.Duration;
import java.time.Instant;

public class DbiBenchmark {
    public static void main(String[] args) {
        try {
            BenchmarkDB db = new BenchmarkDB();
            Instant starts = Instant.now();
            db.createDatabase(1);
            Instant ends = Instant.now();
            System.out.println(Duration.between(starts, ends));
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
