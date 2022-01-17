package org.gorpipe.spark;

import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.spark.platform.*;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class RedisSparkQueryHandler extends GeneralSparkQueryHandler {
    GorClusterBase cluster;
    public static final String queue = "GOR_CLUSTER";

    String sparkRedisUri;
    String key;
    private JedisPool jedisPool;

    public RedisSparkQueryHandler(GorSparkSession gorPipeSession, String sparkRedisUri) {
        super();
        this.sparkRedisUri = sparkRedisUri;
        if (gorPipeSession != null) init(gorPipeSession);
    }

    public void setCluster(GorClusterBase cluster) {
        this.cluster = cluster;
    }

    @Override
    public void init(GorSparkSession gorPipeSession) {
        super.init(gorPipeSession);
        key = gorPipeSession.streamKey();
        if (sparkRedisUri != null && sparkRedisUri.length() > 0) {
            jedisPool = SharedRedisPools.getJedisPool(JedisURIHelper.create(sparkRedisUri));
            gorPipeSession.redisUri_$eq(sparkRedisUri);
            if (cluster == null) {
                cluster = new GeneralSparkCluster(sparkRedisUri);
            }
        }
    }

    @Override
    public String[] executeBatch(String[] fingerprints, String[] commandsToExecute, String[] batchGroupNames, String[] cacheFiles, GorMonitor mon) {
        String[] jobIds = Arrays.copyOf(fingerprints, fingerprints.length);
        if (jedisPool != null) {
            GorLogSubscription subscription = new RedisLogSubscription(cluster, new GorMonitorGorLogForwarder(mon), jobIds, key);
            subscription.start();
        }
        return super.executeBatch(fingerprints,commandsToExecute,batchGroupNames,cacheFiles,mon);
    }
}

