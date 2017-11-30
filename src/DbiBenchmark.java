import db.BenchmarkDB;

public class DbiBenchmark {
    public static void main(String[] args) {
        try {
            BenchmarkDB db = new BenchmarkDB();
            db.createDatabase(10);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
