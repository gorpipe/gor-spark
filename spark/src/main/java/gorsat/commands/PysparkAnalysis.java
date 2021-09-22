package gorsat.commands;

import gorsat.Commands.Analysis;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.spark.GorSparkUtilities;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PysparkAnalysis implements AutoCloseable {
    public static Map<String,PysparkAnalysis> datasetMap = new HashMap<>();
    ExecutorService es;
    Process pythonProcess;
    Dataset<? extends Row> ds;
    List<String> cmds = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    ByteArrayOutputStream baOutput = new ByteArrayOutputStream();
    ByteArrayOutputStream baError = new ByteArrayOutputStream();

    public Dataset<Row> getDataset() {
        return (Dataset<Row>)ds;
    }

    public void setDataset(Dataset<Row> ds) {
        this.ds = ds;
    }

    public void markDone() throws InterruptedException {
        latch.countDown();
        latch = new CountDownLatch(1);
        latch.await();
    }

public class PysparkAnalysis extends Analysis {
    public static Dataset<Row> pyspark(Dataset<? extends Row> ds, String cmd) throws IOException, InterruptedException {
        SparkSession spark = GorSparkUtilities.getSparkSession();
        Py4JServer py4JServer = GorSparkUtilities.getPyServer();

    public Dataset<Row> pyspark(String signature, Dataset<? extends Row> ds, String cmd) throws IOException, InterruptedException {
        datasetMap.put(signature,this);
        this.ds = ds;

        var spark = GorSparkUtilities.getSparkSession();
        var py4JServer = GorSparkUtilities.initPy4jServer();

        var cmdsplit = cmd.trim().split(" ");
        var pysparkDriverPython = System.getenv("PYSPARK_DRIVER_PYTHON");
        var pysparkDriverPythonOpts = "";
        if (pysparkDriverPython!=null) {
            pysparkDriverPythonOpts = System.getenv("PYSPARK_DRIVER_PYTHON_OPTS");
        } else {
            pysparkDriverPython = System.getenv("PYSPARK_PYTHON");
        }
        cmds.add(pysparkDriverPython != null ? pysparkDriverPython : "python3");
        if (pysparkDriverPythonOpts!=null&&pysparkDriverPythonOpts.length()>0) {
            Arrays.stream(pysparkDriverPythonOpts.split(" ")).map(String::trim).forEach(cmds::add);
        }
        cmds.add(cmdsplit[0]);
        cmds.add(signature);
        for (int i = 1; i < cmdsplit.length; i++) cmds.add(cmdsplit[i]);
        ProcessBuilder pb = new ProcessBuilder(cmds);
        Map<String,String> env = pb.environment();
        env.put("PYSPARK_GATEWAY_PORT",Integer.toString(py4JServer.getListeningPort()));
        env.put("PYSPARK_GATEWAY_SECRET",py4JServer.secret());
        env.put("PYSPARK_PIN_THREAD","true");

        Process p = pb.start();
        p.waitFor();

        return spark.sql("select * from input");
    }
}
