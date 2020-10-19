package org.gorpipe.spark.redis;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.GorSparkUtilities;
import java.util.concurrent.*;

public class GorSparkRedisRunner implements Callable<String> {
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
        instance = this;
        this.sparkSession = sparkSession;
        redisUri = GorSparkUtilities.getSparkGorRedisUri();
    }

    @Override
    public String call() throws Exception {
        StructField[] fields = {StructField.apply("_id", DataTypes.StringType, true, Metadata.empty()),StructField.apply("job", DataTypes.StringType, true, Metadata.empty()),StructField.apply("field", DataTypes.StringType, true, Metadata.empty()),StructField.apply("value", DataTypes.StringType, true, Metadata.empty())};
        StructType schema = new StructType(fields);
        RedisBatchConsumer redisBatchConsumer = new RedisBatchConsumer(sparkSession, redisUri);
        StreamingQuery query = sparkSession.readStream().format("redis")
            .option("stream.keys", "resque")
            .schema(schema)
            .load().writeStream().outputMode("update")
            .foreachBatch(redisBatchConsumer).start();
        query.awaitTermination();
        redisBatchConsumer.close();

        return "";
    }

    public static void main(String[] args) {
        GorSparkRedisRunner grr = new GorSparkRedisRunner();
        try {
            grr.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
