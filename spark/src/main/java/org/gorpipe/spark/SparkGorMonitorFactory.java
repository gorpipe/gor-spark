package org.gorpipe.spark;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(SparkMonitorFactory.class)
public class SparkGorMonitorFactory implements SparkMonitorFactory,Serializable {
    @Override
    public SparkGorMonitor createSparkGorMonitor(String jobId,String redisUri) {
        return new SparkGorMonitor(jobId);
    }
}
