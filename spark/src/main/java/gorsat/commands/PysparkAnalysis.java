package gorsat.commands;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gorpipe.exceptions.GorResourceException;
import org.gorpipe.exceptions.GorSystemException;
import org.gorpipe.spark.GorSparkUtilities;

import java.io.ByteArrayOutputStream;
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

    public void waitFor() throws InterruptedException {
        latch.await();
    }

    public Dataset<Row> pyspark(String signature, Dataset<? extends Row> ds, String cmd) throws IOException, InterruptedException {
        datasetMap.put(signature,this);
        this.ds = ds;

        var spark = GorSparkUtilities.getSparkSession();
        var py4JServer = GorSparkUtilities.initPy4jServer();

        var cmdsplit = cmd.trim().split(" ");
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        cmds.add(pysparkPython != null ? pysparkPython : "python3");
        cmds.add(cmdsplit[0]);
        cmds.add(signature);
        for (int i = 1; i < cmdsplit.length; i++) cmds.add(cmdsplit[i]);
        ProcessBuilder pb = new ProcessBuilder(cmds);
        Map<String,String> env = pb.environment();

        var port = Integer.toString(py4JServer.getListeningPort());
        var secret = py4JServer.secret();

        env.put("PYSPARK_GATEWAY_PORT",port);
        env.put("PYSPARK_GATEWAY_SECRET",secret);
        env.put("PYSPARK_PIN_THREAD","true");

        pythonProcess = pb.start();

        es = Executors.newFixedThreadPool(2);
        es.submit(() -> pythonProcess.getErrorStream().transferTo(baOutput));
        es.submit(() -> pythonProcess.getInputStream().transferTo(baError));

        waitFor();
        return getDataset();
    }

    public String cmdString() {
        return String.join(" ", cmds);
    }

    @Override
    public void close() {
        if (pythonProcess!=null) {
            try {
                latch.countDown();
                int exitCode = pythonProcess.waitFor();
                if (exitCode!=0) {
                    throw new GorResourceException("Non zero exit code "+exitCode+"\n"+baOutput+"\n"+baError, cmdString());
                }
            } catch (InterruptedException e) {
                throw new GorSystemException(e);
            }
        }
        if (es!=null) es.shutdown();
    }
}
