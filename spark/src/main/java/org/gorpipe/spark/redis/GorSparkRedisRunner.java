package org.gorpipe.spark.redis;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.GorSparkUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class GorSparkRedisRunner implements Callable<String>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GorSparkRedisRunner.class);
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
        log.info("Initializing GorSparkRedisRunner");

        //sparkSession.sparkContext().setLogLevel("DEBUG");
        instance = this;
        this.sparkSession = sparkSession;
        redisUri = GorSparkUtilities.getSparkGorRedisUri();
    }

    @Override
    public String call() throws Exception {
        log.info("Starting GorSparkRedisRunner");

        StructField[] fields = {StructField.apply("_id", DataTypes.StringType, true, Metadata.empty()),StructField.apply("job", DataTypes.StringType, true, Metadata.empty()),StructField.apply("field", DataTypes.StringType, true, Metadata.empty()),StructField.apply("value", DataTypes.StringType, true, Metadata.empty())};
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
