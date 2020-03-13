package com.nextcode.gor.platform;

/**
 * Interface passable to wait methods
 *
 * @author vilm
 */
public interface JobMonitor {
    /**
     * Called periodically while job is running (passed to waitFor etc).
     *
     * @param job Job
     * @return True if job should keep running
     */
    boolean monitor(GorJob job);

}
