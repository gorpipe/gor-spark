package org.gorpipe.spark.redis

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import com.redislabs.provider.redis.streaming._
import org.gorpipe.spark.GorSparkSession
import org.gorpipe.spark.GorSparkUtilities
import java.util.concurrent.Callable

import org.apache.spark.streaming.StreamingContext


object ScalaGorSparkRedisRunner {
  var instance: ScalaGorSparkRedisRunner = null
}

class ScalaGorSparkRedisRunner() extends Callable[String] {
  ScalaGorSparkRedisRunner.instance = this
  var sparkSession = GorSparkUtilities.getSparkSession(null, null)

  @throws[Exception]
  override def call: String = {
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(2))
    val redisStream = ssc.createRedisStream(Array("resque", "bleh"), storageLevel = StorageLevel.MEMORY_AND_DISK_2)
    redisStream.print()
    ssc.start()
    ssc.awaitTermination()
    /*val query = sparkSession.readStream.format("redis").option("stream.read.block", 2000).option("stream.keys", "resque").load.writeStream.format("console").start
    foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
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
    query.awaitTermination()*/
    ""
  }
}
