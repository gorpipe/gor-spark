package org.gorpipe.spark.redis;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.GorSparkUtilities;

import java.util.List;
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
        StructField[] fields = {StructField.apply("_id", DataTypes.StringType, true, Metadata.empty()),StructField.apply("status", DataTypes.StringType, true, Metadata.empty())};
        StructType schema = new StructType(fields);
        StreamingQuery query = sparkSession.readStream().format("redis")
                .option("stream.read.batch.size",1)
                .option("stream.read.block", 1000)
                .option("stream.keys", "resque")
                //.option("infer.schema", true)
                //.option("inferschema", true)
                .schema(schema)
                .load().writeStream().outputMode("update")
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> v1, Long v2) throws Exception {
                        List<Row> rr = v1.collectAsList();
                        v1.printSchema();
                        rr.forEach(System.err::println);
                    }
                })
                /*.foreach(new ForeachWriter<Row>() {
            @Override
            public boolean open(long partitionId, long epochId) {
                return true;
            }

            @Override
            public void close(Throwable errorOrNull) {

            }

            @Override
            public void process(Row row) {
                System.err.println("hey " + row.toString());
            }
        })*/
                .start();
                /*.foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
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

    public static void main(String[] args) {
        var grr = new GorSparkRedisRunner();
        try {
            grr.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
