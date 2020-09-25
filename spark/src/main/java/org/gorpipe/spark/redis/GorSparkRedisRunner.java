package org.gorpipe.spark.redis;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.GorSparkUtilities;

import java.util.concurrent.Callable;

public class GorSparkRedisRunner implements Callable<String> {
    public static GorSparkRedisRunner instance;
    private SparkSession sparkSession;

    public GorSparkRedisRunner(GorSparkSession sparkSession) {
        instance = this;
        this.sparkSession = sparkSession.getSparkSession();
    }

    public GorSparkRedisRunner() {
        instance = this;
        this.sparkSession = GorSparkUtilities.getSparkSession();
    }

    @Override
    public String call() throws Exception {
        StreamingQuery query = sparkSession.readStream().format("redis").option("stream.read.block", 2000).option("stream.keys", "resque").load().writeStream().format("console").start();
        /*foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> v1, Long v2) throws Exception {
                v1.foreach(new ForeachFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.err.println("hey " + row.toString());
                    }
                });
            }
        }).start();*/
        query.awaitTermination();
        return "";
    }
}
