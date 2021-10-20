package org.gorpipe.spark;

import java.io.Serializable;

public class SparkGorMonitorFactory implements Serializable {
    public SparkGorMonitor createSparkGorMonitor(String jobId) {
        return new SparkGorMonitor(jobId);
    }
}
