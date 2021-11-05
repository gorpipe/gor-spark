package org.gorpipe.spark;

import org.gorpipe.gor.monitor.GorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;

public class SparkGorMonitor extends GorMonitor implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SparkGorMonitor.class);
    String jobId;

    public SparkGorMonitor(String jobId) {
        init(jobId);
    }

    public void init(String jobId) {
        this.jobId = jobId;
    }

    public Duration getJobExpiration() {
        return Duration.ofMinutes(20);
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public void log(String progress) {
        log.debug(progress);
    }
}
