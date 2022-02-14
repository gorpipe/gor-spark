package org.gorpipe.spark.redis;

public class Job {
    private String status;

    public Job(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
