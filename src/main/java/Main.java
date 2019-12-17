import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class Main {

    static String HADOOP_COMMON_PATH = "SET YOUR WINUTILS PATH HERE";

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

        SparkConf conf = new SparkConf().setAppName("SparkGraphs_II").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setCheckpointDir(Files.createTempDir().getAbsolutePath());

        SQLContext sqlctx = new SQLContext(ctx);

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
                Level.ERROR);
//        benchmark.benchma(ctx, sqlctx);
        graphx_bench.graphx_bench(ctx);
    }
}
