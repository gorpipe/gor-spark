package org.gorpipe.spark;

import java.io.Serializable;

public class SparkGorRedisMonitorFactory extends SparkGorMonitorFactory implements Serializable {
    String redisUri;
    public SparkGorRedisMonitorFactory(String redisUri) {
        this.redisUri = redisUri;
    }

    @Override
    public SparkGorMonitor createSparkGorMonitor(String jobId) {
        return new SparkGorRedisMonitor(redisUri, jobId);
    }
}
