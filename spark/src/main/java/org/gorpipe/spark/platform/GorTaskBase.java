package org.gorpipe.spark.platform;

import org.gorpipe.exceptions.ExceptionUtilities;
import org.gorpipe.exceptions.GorException;
import org.gorpipe.exceptions.GorSystemException;
import org.gorpipe.exceptions.GorUserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GorTaskBase implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(GorTaskBase.class);

    private String jobId;
    private GorClusterBase cluster;

    protected GorTaskBase(String jobId) {
        this.jobId = jobId;
    }

    /**
     * Work method. Should return normally if successful. Should raise error if not.
     * Can either return or raise if cancelled.
     *
     * @return result or null if no result
     * @throws Exception on error
     */
    protected abstract String perform() throws Exception;

    /**
     * Get job id
     *
     * @return Job id
     */
    public String getJobId() {
        return jobId;
    }

    protected abstract String getRequestId();

    protected void setProgress(String progress) {
        getCluster().setValue(getJobId(), JobField.Progress, progress);
    }

    protected void setResult(String result) {
        log.debug("Job {} set result: {}", getJobId(), result);
        getCluster().setValue(getJobId(), JobField.Result, result);
    }

    protected void setStatus(JobStatus status) {
        log.info("Job {} set status: {}", getJobId(), status);
        getCluster().setValue(getJobId(), JobField.Status, status.toString());
    }

    protected void setError(String error) {
        log.info("Job {} set error: {}", getJobId(), error);
        getCluster().setValue(getJobId(), JobField.Error, error);
    }

    public void setCluster(GorClusterBase cluster) {
        this.cluster = cluster;
    }

    public GorClusterBase getCluster() {
        return cluster;
    }

    /**
     * Get job status
     *
     * @return Status
     */
    public JobStatus getStatus() {
        return JobStatus.get(getCluster().getValue(getJobId(), JobField.Status));
    }

    /**
     * Get exclusive cluster-wide lock
     *
     * @param key       Key to lock
     * @param timeoutMs Timeout in ms waiting for lock
     * @return true if lock was acquired
     */
    protected boolean lock(String key, long timeoutMs) throws InterruptedException {
        logDebug("Requesting lock " + key);
        boolean gotLock = getCluster().locks().lock(this, key, timeoutMs);
        if (gotLock) {
            logDebug("Got lock " + key);
        } else {
            logDebug("Timout waiting for lock " + key);
        }
        return gotLock;
    }

    /**
     * Release lock
     */
    protected void unlock() {
        getCluster().locks().unlock(this);
    }

    protected boolean hasLock() {
        return getCluster().locks().hasLock(this);
    }

    protected void logInfo(String message, Throwable ex) {
        getCluster().logInfo(getMessage(message), ex);
    }

    protected void logDebug(String message) {
        if (log.isDebugEnabled()) {
            getCluster().logDebug(getMessage(message));
        }
    }

    protected void logWarn(String message, Throwable ex) {
        getCluster().logWarn(getMessage(message), ex);
    }

    protected void logError(String message, Throwable ex) {
        getCluster().logError(getMessage(message), ex);
    }

    protected void logProgress(String progress) {
        getCluster().logProgress(getJobId(), progress);
    }

    protected String getMessage(String message) {
        return message;
    }

    /**
     * Check if job has been cancelled
     *
     * @return true if canncelled
     */
    public boolean isCancelled() {
        return getCluster().getValue(getJobId(), JobField.CancelFlag) != null;
    }

    @Override
    public final void run() {
        try {

            if (isCancelled()) {
                logInfo("Job cancelled before starting", null);
                setStatus(JobStatus.CANCELLED);
                setError("Cancelled before starting");
                return;
            }
            logDebug("STARTUP");
            setStatus(JobStatus.RUNNING);
            setProgress("0");

            String result = perform();
            if (result != null) {
                setResult(result);
            }
            if (isCancelled()) {
                logInfo("CANCEL detected after successful return", null);
                setStatus(JobStatus.CANCELLED);
                logProgress(getProgressMessage("CANCELLED"));
            } else {
                setProgress("100");
                setStatus(JobStatus.DONE);
                logProgress(getProgressMessage("DONE"));
            }
        } catch (Exception e) {

            log.error("Job of ID " + getJobId() + " with status " + getStatus() + " encountered exception", e);

            if (isCancelled()) {
                logInfo("CANCEL detected after exception", null);
                logProgress(getProgressMessage("CANCELLED"));
                setStatus(JobStatus.CANCELLED);
                return;  // Don't want this task reported as failed in cluster mgmt.
            } else {
                logDebug("FAILURE general failure");

                logProgress(getProgressMessage("FAILURE"));
                setStatus(JobStatus.FAILED);
            }

            if (e instanceof GorException) {
                ((GorException)e).setRequestID(getRequestId());
            }

            setError(ExceptionUtilities.gorExceptionToJson(e));

            if (e instanceof GorUserException) {
                logInfo(ExceptionUtilities.gorExceptionToString(e), e);
                throw (GorUserException)e;
            } else {
                logError("Job " + getJobId() + " failed with exception:\n" + ExceptionUtilities.gorExceptionToString(e), e);

                if (e instanceof GorException) {
                    throw (GorException)e;
                } else {
                    throw new GorSystemException(e);
                }
            }
        } finally {
            getCluster().forget(this);
            // Clear interrupted flag if set
            Thread.interrupted();
        }
    }

    protected String getProgressMessage(String state) {
        return "JOB:" + getJobId() + ":" + state;
    }

    /**
     * Get short name for task class.
     *
     * @param taskClass Class
     * @return name without package
     */
    public static String getTaskName(Class<? extends GorTaskBase> taskClass) {
        return taskClass.getName().replaceAll(".*\\.", "");
    }
}
