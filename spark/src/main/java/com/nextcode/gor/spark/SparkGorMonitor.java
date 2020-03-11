package com.nextcode.gor.spark;

import java.io.Serializable;
import java.time.Duration;

import com.nextcode.gor.platform.JedisURIHelper;
import com.nextcode.gor.platform.JobField;
import com.nextcode.gor.platform.SharedRedisPools;
import org.gorpipe.model.genome.files.gor.GorMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class SparkGorMonitor extends GorMonitor implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SparkGorMonitor.class);

    private JedisPool jedisPool;
    private String uri;
    private String jobId;
    boolean working = true;

    public SparkGorMonitor(String uri, String jobId) {
        this.uri = uri;
        this.jobId = jobId;
        try {
            jedisPool = SharedRedisPools.getJedisPool(JedisURIHelper.create(uri));
        } catch (Exception e) {
            working = false;
        }
    }

    public String getRedisUri() {
        return uri;
    }

    private String getJobKey(String jobId) {
        return getKey("JOB", jobId);
    }

    public Duration getJobExpiration() {
        return Duration.ofMinutes(20);
    }

    public String getValue(JobField field) {
        if (working) {
            try (Jedis jedis = jedisPool.getResource()) {
                String jobKey = getJobKey(jobId);
                return jedis.hget(jobKey, field.key());
            } catch (JedisConnectionException jce) {
                if (!jce.getMessage().contains("SocketTimeout")) {
                    working = false;
                }
                logError("Updating progress resulted in an error", jce);
            } catch (ClassCastException cce) {
                log.debug("Unable to get a connection to redis at " + uri, cce);
            } catch (Exception jde) {
                working = false;
                log.debug("Unable to get a connection to redis at " + uri);
            }
        }
        return null;
    }

    public void setValue(JobField field, String value) {
        if (!working) {
            return;
        }
        try {
            try (Jedis jedis = jedisPool.getResource()) {
                String jobKey = getJobKey(jobId);
                jedis.hset(jobKey, field.key(), value);
                // We want the job key to live longer than the job it self.
                jedis.expire(jobKey, (int) getJobExpiration().getSeconds());
            }
        } catch (Exception jde) {
            working = false;
            log.debug("Unable to get a connection to redis at localhost:6379");
        }
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void logError(String message, Throwable th) {
        log.error(message, th);
    }

    String getKey(String... parts) {
        return "resque:DC:" + String.join(":", parts);
    }

    String getPrivateLogKey(String jobId) {
        return getKey("JOB", jobId, "LOG");
    }

    @Override
    public void log(String progress) {
        if (!working) {
            return;
        }
        try {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(getPrivateLogKey(jobId), progress);
            }
        } catch (JedisConnectionException jce) {
            if (!jce.getMessage().contains("SocketTimeout")) {
                working = false;
            }
            logError("Updating progress resulted in an error", jce);
        } catch (Exception e) {
            working = false;
            logError("Updating progress resulted in an error", e);
        }
    }
}
