package org.gorpipe.spark;

import com.google.auto.service.AutoService;
import java.io.Serializable;

@AutoService(SparkMonitorFactory.class)
public class SparkGorRedisMonitorFactory implements SparkMonitorFactory,Serializable {
    @Override
    public SparkGorMonitor createSparkGorMonitor(String jobId,String redisUri) {
        return new SparkGorRedisMonitor(redisUri, jobId);
    }
}
