package org.gorpipe.spark.redis;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.GorSparkUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;

public class GorSparkRedisRunner implements Callable<String>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GorSparkRedisRunner.class);
    private static final String CUSTOM_SPARK_LOGLEVEL_CONFIG = "spark.logLevel";
    private static final String DEFAULT_LOG_LEVEL = "WARN";
    public static int DEFAULT_TIMEOUT = 3600;
    public static int DEFAULT_DRIVER_TIMEOUT_MINUTES = 60*12;
    public static GorSparkRedisRunner instance;
    private SparkSession sparkSession;
    private String redisUri;
    private String streamKey;
    private int timeoutCount;
    private int driverTimeoutMinutes;

    public GorSparkRedisRunner(GorSparkSession sparkSession) {
        init(sparkSession.getSparkSession(), "resque", DEFAULT_TIMEOUT);
    }

    public GorSparkRedisRunner() {
        init(GorSparkUtilities.getSparkSession(), "resque", DEFAULT_TIMEOUT);
    }

    public GorSparkRedisRunner(String streamKey, int timeoutCount) {
        init(GorSparkUtilities.getSparkSession(), streamKey, timeoutCount);
    }

    public void init(SparkSession sparkSession, String streamKey, int timeoutCount) {
        SparkContext context = sparkSession.sparkContext();
        SparkConf conf = context.conf();
        String logLevel = DEFAULT_LOG_LEVEL;
        if(conf.contains(CUSTOM_SPARK_LOGLEVEL_CONFIG)) logLevel = conf.get(CUSTOM_SPARK_LOGLEVEL_CONFIG);
        context.setLogLevel(logLevel);

        log.info("Initializing GorSparkRedisRunner");

        var driverTimeout = System.getenv("GOR_SPARK_DRIVER_TIMEOUT_MINUTES");
        this.driverTimeoutMinutes = Objects.nonNull(driverTimeout) && driverTimeout.length() > 0 ? Integer.parseInt(driverTimeout) : DEFAULT_DRIVER_TIMEOUT_MINUTES;
        this.streamKey = streamKey;
        this.timeoutCount = timeoutCount;
        instance = this;
        this.sparkSession = sparkSession;
        redisUri = GorSparkUtilities.getSparkGorRedisUri();
    }

    @Override
    public String call() throws Exception {
        log.info("Starting GorSparkRedisRunner");

        StructField[] fields = {StructField.apply("_id", DataTypes.StringType, true, Metadata.empty()),StructField.apply("job", DataTypes.StringType, true, Metadata.empty()),StructField.apply("field", DataTypes.StringType, true, Metadata.empty()),StructField.apply("value", DataTypes.StringType, true, Metadata.empty()),StructField.apply("sec", DataTypes.StringType, true, Metadata.empty())};
        StructType schema = new StructType(fields);
        try(RedisBatchConsumer redisBatchConsumer = new RedisBatchConsumer(sparkSession, redisUri, streamKey)) {
            StreamingQuery query = sparkSession.readStream().format("redis")
                    .option("stream.keys", streamKey)
                    .schema(schema)
                    .load().writeStream().outputMode("update")
                    .foreachBatch(redisBatchConsumer).start();
            log.info("GorSparkRedisRunner is running");
            redisBatchConsumer.mont.setQuery(query, timeoutCount);
            query.awaitTermination(Duration.ofMinutes(driverTimeoutMinutes).toMillis());
        }
        log.info("GorSparkRedisRunner has stopped");

        return "";
    }

    public static void main(String[] args) {
        var streamKey = args.length > 0 ? args[0] : "resque";
        var timeoutCount = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_TIMEOUT;

        if (args.length>0) {
            var split = streamKey.split(";");
            if (split.length>1) {
                var projectPath = Path.of(split[1]);
                //Files.createSymbolicLink(Path.of(System.getProperty("user.home")).resolve("project"), projectPath);
            }
        }

        try(GorSparkRedisRunner grr = new GorSparkRedisRunner(streamKey, timeoutCount)) {
            grr.call();
        } catch (Exception e) {
            log.error("Error running GorSparkRedisRunner", e);
        }
    }

    @Override
    public void close() {
        log.info("Closing spark session");
        if(sparkSession!=null) sparkSession.close();
    }
}
