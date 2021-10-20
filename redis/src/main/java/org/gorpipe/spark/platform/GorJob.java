package org.gorpipe.spark.platform;

import java.time.Duration;
import java.util.Map;

public interface GorJob {

    /**
     * Get job status
     *
     * @return Status
     */
    JobStatus getStatus();

    /**
     * Wait for job completion.
     *
     * @param giveUp       If true, throw exception if failed/timeout
     * @param monitor      to receive callbacks
     * @param jobRetention how long can the job live.
     * @return true if job completed successfully,false if failure or timeout
     * @throws InterruptedException if interrupted
     */
    default boolean waitFor(boolean giveUp, JobMonitor monitor, Duration jobRetention) throws InterruptedException {
        int periodMs = 100;
        return waitFor((int) jobRetention.toMillis() / periodMs, periodMs, giveUp, monitor);
    }

    /**
     * Wait for job completion
     *
     * @param count    Number of periods
     * @param periodMs Period length in ms
     * @param giveUp   If true, throw exception if failed/timeout
     * @param monitor  Monitor to receive callbacks
     * @return true if job completed successfully,false if failure or timeout
     * @throws InterruptedException if interrupted
     */
    boolean waitFor(int count, int periodMs, boolean giveUp, JobMonitor monitor) throws InterruptedException;

    /**
     * Signal job for cancel
     */
    void cancel();

    /**
     * Check if job is cancelling.
     * @return true if job is cancelling (cancel has been called but the status is not yet CANCELLED), false otherwise.
     */
    boolean isCancelling();

    /**
     * Get job progress (0..100)
     *
     * @return progress
     */
    int getProgress();

    /**
     * Get job result
     *
     * @return result or null if none
     */
    String getResult();

    /**
     * Get error message
     *
     * @return Error message or null if none
     */
    String getError();

    /**
     * Get client job id (unique across multiple clusters)
     *
     * @return client job id.  Base64 encode job url redis uri/jobid
     */
    String getClientJobId();

    /**
     * Get job id (cluster unique)
     *
     * @return Job id
     */
    String getJobId();

    /**
     * Get submit timestamp
     *
     * @return submit timestamp
     */
    long getSubmitTimestamp();

    /**
     * Get expiration timestamp
     *
     * @return expiration timestamp
     */
    long getExpirationTimestamp();

    /**
     * Check if this gorjob has a submit timestamp
     *
     * @return true if there is a submit timestamp
     */
    boolean hasSubmitTimestamp();

    /**
     * Check if this gorjob has an expiration timestamp
     *
     * @return true if there is an expiration timestamp
     */
    boolean hasExpirationTimestamp();


    /**
     * Get the job query.
     *
     * TODO:  This is the only GOR specific method on this interface, but it is called GorJob.  Maybe we should
     *        move this method to child class of ResqueJob (which is child of this job).
     *
     * @return the query.
     */
    GorQuery getQuery();

    /**
     * Convert job data to map
     *
     * @return map
     */
    Map<String, String> toMap();
}
