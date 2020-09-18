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
    public static Dataset<Row> pyspark(Dataset<? extends Row> ds, String cmd) throws IOException, InterruptedException {
        SparkSession spark = GorSparkUtilities.getSparkSession(null,null);
        Py4JServer py4JServer = GorSparkUtilities.getPyServer();
        
        ProcessBuilder pb = new ProcessBuilder("python3", cmd.trim());
        Map<String,String> env = pb.environment();
        env.put("PYSPARK_GATEWAY_PORT",Integer.toString(py4JServer.getListeningPort()));
        env.put("PYSPARK_GATEWAY_SECRET",py4JServer.secret());
        env.put("PYSPARK_PIN_THREAD","true");

        Process p = pb.start();
        String error = new String(p.getErrorStream().readAllBytes());
        int exitCode = p.waitFor();
        System.err.println("exitCode " + exitCode + " " + error);
        //return (Dataset<Row>)ds.filter("gene_symbol = 'BRCA2'");
        return spark.sql("select * from input");
    }
}
