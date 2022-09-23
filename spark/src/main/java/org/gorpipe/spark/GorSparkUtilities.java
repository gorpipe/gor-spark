package org.gorpipe.spark;

//import io.projectglow.GlowBase;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.api.r.RAuthHelper;
import org.apache.spark.api.r.RBackend;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.model.Row;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.spark.udfs.*;
import org.gorpipe.util.standalone.GorStandalone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Paths;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GorSparkUtilities {
    private static final Logger log = LoggerFactory.getLogger(GorSparkUtilities.class);
    private static SparkSession spark;
    private static Py4JServer py4jServer;
    private static RBackend rBackend;
    private static Optional<Process> jupyterProcess;
    private static Optional<String> jupyterPath = Optional.empty();
    private static Optional<String> rPath = Optional.empty();
    private static ExecutorService es;

    private GorSparkUtilities() {}
    public static Py4JServer getPyServer() {
        return py4jServer;
    }

    public static RBackend getRBackend() {
        return rBackend;
    }

    public static int getPyServerPort() {
        return py4jServer != null ? py4jServer.getListeningPort() : 0;
    }

    public static String getPyServerSecret() {
        return py4jServer != null ? py4jServer.secret() : "";
    }

    public static Optional<String> getJupyterPath() {
        return jupyterPath;
    }

    public static Optional<String> parseDriverId() {
        return getJupyterBaseUrl().map(u -> u.substring("jupyter-sparkgor-".length()));
    }

    public static Optional<String> getRPath() {
        return rPath;
    }

    public static void closePySpark() {
        try {
            jupyterProcess.ifPresent(Process::destroy);
        } finally {
            try {
                shutdownPy4jServer();
                if (rBackend != null) rBackend.close();
            } finally {
                if (es != null) es.shutdown();
            }
        }
    }

    public static void shutdownPy4jServer() {
        if(py4jServer!=null) py4jServer.shutdown();
    }

    public static Py4JServer initPy4jServer() {
        if (py4jServer==null) {
            py4jServer = new Py4JServer(spark.sparkContext().conf());
            py4jServer.start();
        }
        return py4jServer;
    }

    static synchronized void setJupyterPath(String jp) {
        jupyterPath = Optional.of(jp);
        spark.createDataset(Collections.singletonList(jp), Encoders.STRING()).createOrReplaceTempView("jupyterpath");
    }

    private static Optional<String> getJupyterBaseUrl() {
        var baseurl = System.getenv("JUPYTER_BASE_URL");
        if(baseurl==null) baseurl = System.getProperty("JUPYTER_BASE_URL");
        if (baseurl!=null&&!baseurl.isEmpty()) {
            return Optional.of(baseurl);
        }
        return Optional.empty();
    }

    public static void initPySpark(Optional<String> standaloneRoot) {
        int rbackendPort = -1;
        String rbackendSecret = null;
        var sparkr = System.getenv("SPARKR_INIT");
        if(sparkr==null) sparkr = System.getProperty("SPARKR_INIT");
        if (rBackend==null&&sparkr!=null&&sparkr.length()>0) {
            rBackend = new RBackend();
            Tuple2<Object, RAuthHelper> tuple = rBackend.init();
            rbackendPort = (Integer)tuple._1;
            rbackendSecret = tuple._2.secret();
            rPath = Optional.of(rbackendPort+";"+rbackendSecret);
            System.err.println(rPath);

            System.err.println("Sys.setenv(\"JAVA_HOME\" = \"/Library/Java/JavaVirtualMachines/graalvm-ce-java17-22.2.0/Contents/Home\")\n" +
                    "Sys.setenv(\"SPARK_HOME\" = \"/opt/homebrew/Cellar/apache-spark/3.3.0/libexec\")" +
                    "Sys.setenv(\"EXISTING_SPARKR_BACKEND_PORT\" = \""+rbackendPort+"\")" +
                    "Sys.setenv(\"SPARKR_BACKEND_AUTH_SECRET\" = \""+rbackendSecret+"\")" +
                    "library(SparkR, lib.loc = c(file.path(Sys.getenv(\"SPARK_HOME\"), \"R\", \"lib\")))" +
                    "sparkR.session()");

            new Thread(() -> rBackend.run()).start();
        }

        var pyspark = System.getenv("PYSPARK_PIN_THREAD");
        if(pyspark==null) pyspark = System.getProperty("PYSPARK_PIN_THREAD");
        if (py4jServer==null&&pyspark!=null&&pyspark.length()>0) {
            initPy4jServer();
            var spark = GorSparkUtilities.getSparkSession();

            var plist = new ArrayList<>(List.of("jupyter-lab", "--ip=0.0.0.0", "--NotebookApp.allow_origin='*'","--port=8888","--NotebookApp.port_retries=0"));
            var notebookdir = System.getenv("JUPYTER_NOTEBOOK_DIR");
            if(notebookdir==null) notebookdir = System.getProperty("JUPYTER_NOTEBOOK_DIR");
            if (notebookdir!=null&&!notebookdir.isEmpty()) {
                plist.add("--notebook-dir="+notebookdir);
            }
            var baseurlOpt = getJupyterBaseUrl();
            if (baseurlOpt.isPresent()) {
                var baseurl = baseurlOpt.get();
                plist.add("--NotebookApp.base_url=/"+baseurl);
                plist.add("--LabApp.base_url=/"+baseurl);
            }

            var pyServerPort = Integer.toString(GorSparkUtilities.getPyServerPort());
            var pyServerSecret = GorSparkUtilities.getPyServerSecret();
            System.err.println(pyServerPort+";"+pyServerSecret);

            ProcessBuilder pb = new ProcessBuilder(plist);
            standaloneRoot.ifPresent(sroot -> pb.directory(Paths.get(sroot).toFile()));
            Map<String,String> env = pb.environment();
            env.put("PYSPARK_GATEWAY_PORT",pyServerPort);
            env.put("PYSPARK_GATEWAY_SECRET",pyServerSecret);
            env.put("PYSPARK_PIN_THREAD","true");
            if (rbackendPort>0) {
                env.put("SPARKR_WORKER_PORT",String.valueOf(rbackendPort));
                env.put("SPARKR_WORKER_SECRET",rbackendSecret);
                env.put("EXISTING_SPARKR_BACKEND_PORT",String.valueOf(rbackendPort));
                env.put("SPARKR_BACKEND_AUTH_SECRET",rbackendSecret);
            }
            try {
                Process p = pb.start();
                jupyterProcess = Optional.of(p);

                es = Executors.newFixedThreadPool(2);
                Future<String> resin = es.submit(() -> {
                    try (InputStream is = p.getInputStream()) {
                        InputStreamReader isr = new InputStreamReader(is);
                        BufferedReader br = new BufferedReader(isr);
                        br.lines().peek(System.err::println).map(String::trim).filter(s -> s.startsWith("http://") && s.contains("?token=")).forEach(GorSparkUtilities::setJupyterPath);
                    }
                    return null;
                });
                Future<String> reserr = es.submit(() -> {
                    try (InputStream is = p.getErrorStream()) {
                        InputStreamReader isr = new InputStreamReader(is);
                        BufferedReader br = new BufferedReader(isr);
                        br.lines().peek(System.err::println).map(String::trim).filter(s -> s.startsWith("http://") && s.contains("?token=")).forEach(GorSparkUtilities::setJupyterPath);
                    }
                    return null;
                });
            } catch(IOException ie) {
                log.info(ie.getMessage());
                jupyterProcess = Optional.empty();
            }
        }
    }

    private static String constructRedisUri(String sparkRedisHost) {
        final String sparkRedisPort = System.getProperty("spark.redis.port");
        final String sparkRedisDb = System.getProperty("spark.redis.db");
        String ret = sparkRedisHost + ":" + (sparkRedisPort != null && sparkRedisPort.length() > 0 ? sparkRedisPort : "6379");
        return sparkRedisDb!=null && sparkRedisDb.length()>0 ? ret + "/" + sparkRedisDb : ret;
    }

    public static String getSparkGorRedisUri() {
        final String sparkRedisHost = System.getProperty("spark.redis.host");
        return sparkRedisHost != null && sparkRedisHost.length() > 0 ? constructRedisUri(sparkRedisHost) : "";
    }

    public static GorMonitor getSparkGorMonitor(String jobId, String redisUri, String key) {
        if (SparkGorMonitor.localProgressMonitor != null) {
            return SparkGorMonitor.localProgressMonitor;
        } else {
            List<ServiceLoader.Provider<SparkMonitorFactory>> srvList;
            srvList = ServiceLoader.load(SparkMonitorFactory.class).stream().toList();
            if (srvList.size() > 0) {
                SparkMonitorFactory sparkMonitorFactory;
                sparkMonitorFactory = srvList.get(0).get();
                if (srvList.size() > 1 && sparkMonitorFactory instanceof SparkGorMonitorFactory && redisUri != null && redisUri.length() > 0) {
                    sparkMonitorFactory = srvList.get(1).get();
                }
                return sparkMonitorFactory.createSparkGorMonitor(jobId, redisUri, key);
            }
            return null;
        }
    }

    private static SparkSession newSparkSession(int workers) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.sql.execution.arrow.pyspark.enabled","true");
        sparkConf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","false");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");
        sparkConf.set("spark.kubernetes.appKillPodDeletionGracePeriod","20");
        //sparkConf.set("spark.hadoop.fs.s3a.endpoint","localhost:4566");
        sparkConf.set("spark.hadoop.fs.s3a.connection.ssl.enabled","false");
        sparkConf.set("spark.hadoop.fs.s3a.path.style.access","true");
        sparkConf.set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
        sparkConf.set("spark.hadoop.fs.s3a.change.detection.mode","warn");
        sparkConf.set("spark.hadoop.com.amazonaws.services.s3.enableV4","true");
        sparkConf.set("spark.hadoop.fs.s3a.committer.name","partitioned");
        sparkConf.set("spark.hadoop.fs.s3a.committer.staging.conflict-mode","replace");
        sparkConf.set("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore");
        //sparkConf.set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
        SparkSession.Builder ssb = SparkSession.builder();
        if(!sparkConf.contains("spark.master")) {
            ssb = workers>0 ? ssb.master("local["+workers+"]") : ssb.master("local[*]");
        }
        SparkSession spark = ssb.config(sparkConf).getOrCreate();

        spark.udf().register("chartodoublearray", new CharToDoubleArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("chartodoublearrayparallel", new CharToDoubleArrayParallel(), DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("todoublearray", new CommaToDoubleArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("todoublematrix", new CommaToDoubleMatrix(), SQLDataTypes.MatrixType());
        spark.udf().register("tointarray", new CommaToIntArray(), DataTypes.createArrayType(DataTypes.IntegerType));

        spark.udf().register("double_array_to_vector", new ArrayToVector(), SQLDataTypes.VectorType());
        spark.udf().register("vector_to_double_array", new VectorToArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        //GlowBase gb = new GlowBase();
        //gb.register(spark, false);

        return spark;
    }

    public static SparkSession getSparkSession() {
        return getSparkSession(0);
    }

    public static SparkSession getSparkSession(int workers) {
        if(spark==null) {
            if (!SparkSession.getDefaultSession().isEmpty()) {
                log.info("SparkSession from default");
                spark = SparkSession.getDefaultSession().get();
            } else {
                log.info("Starting a new SparkSession");
                spark = newSparkSession(workers);
            }
            Optional<String> standaloneRoot = GorStandalone.isStandalone() ? Optional.of(GorStandalone.getStandaloneRoot()) : Optional.empty();
            initPySpark(standaloneRoot);
        }
        return spark;
    }

    public static List<org.apache.spark.sql.Row> stream2SparkRowList(Stream<Row> str, StructType schema) {
        return str.map(p -> new SparkGorRow(p, schema)).collect(Collectors.toList());
    }
}
