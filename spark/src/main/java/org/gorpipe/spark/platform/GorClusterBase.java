package org.gorpipe.spark.platform;

import org.gorpipe.gor.model.GorParallelQueryHandler;
import org.gorpipe.gor.monitor.GorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class GorClusterBase implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GorClusterBase.class);
    private String localHostName;
    private Runnable onWorkerException;

    public abstract Duration getJobRetention();
    public abstract Duration getJobExpiration();
    public abstract GorClusterConfig getConfig();

    public abstract void logInfo(String message, Throwable th);
    public abstract void logDebug(String message);
    public abstract void logWarn(String message, Throwable th);
    public abstract void logError(String message, Throwable th);
    public abstract void logProgress(String jobId, String progress);

    public abstract String getValue(String jobId, JobField field);
    public abstract void setValue(String jobId, JobField field, String value);
    public abstract void yieldFor(Collection<GorJob> jobs, String queue, JobMonitor monitor) throws InterruptedException;

    public abstract GorLocks locks();

    public abstract JedisPool pool();

    public abstract GorLogSubscription addLogSubscription(GorLogReceiver receiver, String... jobIds);

    public abstract GorJob findJob(String jobId);
    public abstract Map<JobStatus,Integer> getJobStatusesByRequestId(String requestId);
    public abstract String getJobMessage(String requestId);

    public abstract void subscribeGorMonitor(String requestId, GorMonitor gorMonitor);

    public abstract boolean jobExists(String jobId);
    public abstract GorJob findJobByFingerprint(String fingerprint, Function<URI, GorClusterBase> clusterLookup);
    public abstract void registerJobByFingerprint(String fingerprint, GorJob job);


    protected abstract void forget(GorTaskBase task);

    public abstract Statistics getCurrentStatistics();

    public abstract GorJob submit(String queue, String task, Object... args);
    public abstract List<GorJob> submit(String queue, BatchSubmission submission);
    public abstract GorJob prioritySubmit(String queue, String task, Object... args);

    public abstract void addScore(String partition, int seconds);

    public abstract String storeSecurityContext(String securityContext);
    public abstract String getSecurityContext(String key);

    public abstract GorParallelQueryHandler createQueryHandler(boolean workOff, GorQuery query, String securityContext);

    // TODO: Delete - just used when running GorServer
    public class Statistics {
        public long workerCount;
        public long pendingJobCount;
        public long runningJobCount;
    }

    public abstract Object createWorker();
    public abstract Object createWorker(String queue);

    /**
     * Wait for jobs to finish
     *
     * @param jobs    Jobs to wait for
     * @param monitor To receive callbacks
     * @throws InterruptedException if interrupted
     */
    public void waitFor(Iterable<GorJob> jobs, JobMonitor monitor) throws InterruptedException {
        try {
            for (GorJob job : jobs) {
                job.waitFor(true, monitor, getJobRetention());
            }
        } catch (InterruptedException | RuntimeException ie) {
            cancel(jobs);
            throw ie;
        }
    }

    /**
     * Do best effort of canceling a list of jobs.
     * Exceptions will be ignored but logged.
     */
    public static void cancel(Iterable<GorJob> jobs) {
        for (GorJob job : jobs) {
            try {
                job.cancel();
            } catch (Exception e) {
                log.info("Ignoring exception caught in cancel", e);
            }
        }
    }

    protected String getLocalHostName() {
        if (localHostName == null) {
            try {
                localHostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Cannot get local host name", e);
            }
        }
        return localHostName;
    }

    /**
     * Worker should notify cluster that it is exiting because of exception.
     *
     * @param source Object
     * @param t      Exception
     */
    public void workerFatalException(Object source, Throwable t) {
        if (onWorkerException != null) {
            log.info("Calling worker exception hook");
            onWorkerException.run();
        }
    }

    /**
     * Can register (single) runnable to run on worker exception
     *
     * @param runnable Runnable action
     */
    public void onWorkerException(Runnable runnable) {
        onWorkerException = runnable;
    }
}
