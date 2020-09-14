package gorsat.commands;

import gorsat.Commands.Analysis;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.spark.GorSparkUtilities;

import java.io.IOException;
import java.util.Map;

public class PysparkAnalysis extends Analysis {
    static Py4JServer py4JServer = null;

    public static Dataset<Row> pyspark(Dataset<? extends Row> ds, String cmd) throws IOException, InterruptedException {
        SparkSession spark = GorSparkUtilities.getSparkSession(null,null);
        if(py4JServer == null) {
            py4JServer = new Py4JServer(spark.sparkContext().conf());
            py4JServer.start();
        }

        ProcessBuilder pb = new ProcessBuilder("python3", cmd.trim());
        Map<String,String> env = pb.environment();
        env.put("PYSPARK_GATEWAY_PORT",Integer.toString(py4JServer.getListeningPort()));
        env.put("PYSPARK_GATEWAY_SECRET",py4JServer.secret());
        env.put("PYSPARK_PIN_THREAD","true");

        Process p = pb.start();
        p.waitFor();

        return spark.sql("select * from input");
    }
}
