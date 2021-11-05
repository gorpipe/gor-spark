package org.gorpipe.spark;

public interface SparkMonitorFactory {
    SparkGorMonitor createSparkGorMonitor(String jobId);
}
