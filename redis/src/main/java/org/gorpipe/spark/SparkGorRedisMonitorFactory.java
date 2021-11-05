package org.gorpipe.spark;

import com.google.auto.service.AutoService;
import java.io.Serializable;

@AutoService(SparkMonitorFactory.class)
public class SparkGorRedisMonitorFactory implements SparkMonitorFactory,Serializable {
    String redisUri;
    public SparkGorRedisMonitorFactory(String redisUri) {
        this.redisUri = redisUri;
    }

    @Override
    public SparkGorMonitor createSparkGorMonitor(String jobId) {
        return new SparkGorRedisMonitor(redisUri, jobId);
    }
}
