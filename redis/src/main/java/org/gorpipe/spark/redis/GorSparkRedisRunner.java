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

import java.util.concurrent.Callable;

public class GorSparkRedisRunner implements Callable<String>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GorSparkRedisRunner.class);
    private static final String CUSTOM_SPARK_LOGLEVEL_CONFIG = "spark.logLevel";
    private static final String DEFAULT_LOG_LEVEL = "WARN";
    public static GorSparkRedisRunner instance;
    private SparkSession sparkSession;
    private String redisUri;

    public GorSparkRedisRunner(GorSparkSession sparkSession) {
        init(sparkSession.getSparkSession());
    }

    public GorSparkRedisRunner() {
        init(GorSparkUtilities.getSparkSession());
    }

    public void init(SparkSession sparkSession) {
        SparkContext context = sparkSession.sparkContext();
        SparkConf conf = context.conf();
        String logLevel = DEFAULT_LOG_LEVEL;
        if(conf.contains(CUSTOM_SPARK_LOGLEVEL_CONFIG)) logLevel = conf.get(CUSTOM_SPARK_LOGLEVEL_CONFIG);
        context.setLogLevel(logLevel);

        log.info("Initializing GorSparkRedisRunner");

        instance = this;
        this.sparkSession = sparkSession;
        redisUri = GorSparkUtilities.getSparkGorRedisUri();
    }

    @Override
    public String call() throws Exception {
        log.info("Starting GorSparkRedisRunner");

        StructField[] fields = {StructField.apply("_id", DataTypes.StringType, true, Metadata.empty()),StructField.apply("job", DataTypes.StringType, true, Metadata.empty()),StructField.apply("field", DataTypes.StringType, true, Metadata.empty()),StructField.apply("value", DataTypes.StringType, true, Metadata.empty()),StructField.apply("sec", DataTypes.StringType, true, Metadata.empty())};
        StructType schema = new StructType(fields);
        try(RedisBatchConsumer redisBatchConsumer = new RedisBatchConsumer(sparkSession, redisUri)) {
            StreamingQuery query = sparkSession.readStream().format("redis")
                    .option("stream.keys", "resque")
                    .schema(schema)
                    .load().writeStream().outputMode("update")
                    .foreachBatch(redisBatchConsumer).start();
            log.info("GorSparkRedisRunner is running");
            query.awaitTermination();
        }
        log.info("GorSparkRedisRunner has stopped");

        return "";
    }

    public static void main(String[] args) {
        try(GorSparkRedisRunner grr = new GorSparkRedisRunner()) {
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
