package org.gorpipe.spark.redis;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.gorpipe.spark.GorSparkSession;

import java.util.concurrent.Callable;

public class GorSparkRedisRunner implements Callable<String> {
    public static GorSparkRedisRunner instance;
    private SparkSession sparkSession;

    public GorSparkRedisRunner(GorSparkSession sparkSession) {
        instance = this;
        this.sparkSession = sparkSession.sparkSession();
    }

    @Override
    public String call() throws Exception {
        StreamingQuery query = sparkSession.readStream().format("redis").load().writeStream().foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> v1, Long v2) throws Exception {
                v1.foreach(new ForeachFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.err.println("hey " + row.toString());
                    }
                });
            }
        }).start();
        query.awaitTermination();
        return "";
    }
}
