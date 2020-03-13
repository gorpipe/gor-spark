package org.gorpipe.spark;

import org.gorpipe.exceptions.GorSystemException;
import org.gorpipe.model.genome.files.gor.GorMonitor;
import org.gorpipe.model.genome.files.gor.GorParallelQueryHandler;
import org.gorpipe.spark.platform.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class GeneralSparkCluster extends GorClusterBase {

    private static final Logger log = LoggerFactory.getLogger(GeneralSparkCluster.class);

    private static final String LOG_CHANNEL = "DC:CLUSTER:LOG";
    private JedisPool jedisPool = null;
    private final String redisUri;

    String logPrefix;

    public GeneralSparkCluster(String sparkRedisUri) {
        this.redisUri = sparkRedisUri;
        if (this.redisUri.length() > 0) {
            jedisPool = SharedRedisPools.getJedisPool(JedisURIHelper.create(this.redisUri));
        }
    }

    @Override
    public Duration getJobExpiration() {
        return null;
    }

    @Override
    public GorClusterConfig getConfig() {
        return null;
    }

    @Override
    public void logInfo(String message, Throwable ex) {
        logRaw(getLogPrefix() + message, ex);
    }

    @Override
    public void logDebug(String message) {

    }

    private GorSystemException getConnectionGorSystemException(JedisConnectionException jde) {
        return new GorSystemException("Unable to get a connection to redis at " + redisUri, jde);
    }

    private void logRaw(String message, Throwable ex) {
        if (ex == null) {
            log.info(message);
        } else {
            log.info(message, ex);
        }

        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(LOG_CHANNEL, message);
            } catch (JedisConnectionException jde) {
                jedisPool = null;
                //throw getConnectionGorSystemException(jde);
            }
        }
    }

    private String getLogPrefix() {
        if (logPrefix == null) {
            try {
                logPrefix = getLocalHostName() + ":";
            } catch (Exception e) {
                log.warn("Cannot get host name", e);
                logPrefix = "<UNKNOWN>:";
            }
        }
        return logPrefix;
    }

    @Override
    public void logWarn(String message, Throwable ex) {
        if (ex == null) {
            log.warn(getLogPrefix() + message);
        } else {
            log.warn(getLogPrefix() + message, ex);
        }
    }

    @Override
    public void logError(String message, Throwable th) {

    }

    @Override
    public void logProgress(String jobId, String progress) {

    }

    @Override
    public String getValue(String jobId, JobField field) {
        return null;
    }

    @Override
    public void setValue(String jobId, JobField field, String value) {

    }

    @Override
    public void yieldFor(Collection<GorJob> jobs, String queue, JobMonitor monitor) {

    }

    @Override
    public GorLocks locks() {
        return null;
    }

    @Override
    public JedisPool pool() {
        return jedisPool;
    }

    @Override
    public GorLogSubscription addLogSubscription(GorLogReceiver receiver, String... jobIds) {
        return null;
    }

    @Override
    public GorJob findJob(String jobId) {
        return null;
    }

    @Override
    public Map<JobStatus, Integer> getJobStatusesByRequestId(String requestId) {
        return null;
    }

    @Override
    public String getJobMessage(String requestId) {
        return null;
    }

    @Override
    public void subscribeGorMonitor(String requestId, GorMonitor gorMonitor) {

    }

    @Override
    public boolean jobExists(String jobId) {
        return false;
    }

    @Override
    public GorJob findJobByFingerprint(String fingerprint, Function<URI, GorClusterBase> clusterLookup) {
        return null;
    }

    @Override
    public void registerJobByFingerprint(String fingerprint, GorJob job) {

    }

    @Override
    protected void forget(GorTaskBase task) {

    }

    @Override
    public GorClusterBase.Statistics getCurrentStatistics() {
        return null;
    }

    @Override
    public GorJob submit(String queue, String task, Object... args) {
        return null;
    }

    @Override
    public List<GorJob> submit(String queue, BatchSubmission submission) {
        return null;
    }

    @Override
    public GorJob prioritySubmit(String queue, String task, Object... args) {
        return null;
    }

    @Override
    public void addScore(String partition, int seconds) {

    }

    @Override
    public String storeSecurityContext(String securityContext) {
        return null;
    }

    @Override
    public String getSecurityContext(String key) {
        return null;
    }

    @Override
    public GorParallelQueryHandler createQueryHandler(boolean workOff, GorQuery query, String securityContext) {
        return null;
    }

    @Override
    public Object createWorker() {
        return null;
    }

    @Override
    public Object createWorker(String queue) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void run() {

    }

    @Override
    public Duration getJobRetention() {
        return Duration.ofMillis(3600000);
    }
}
