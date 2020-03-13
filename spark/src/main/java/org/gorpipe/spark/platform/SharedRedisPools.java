package org.gorpipe.spark.platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.net.URI;

/**
 * Share redis pools
 *
 * @author vilm
 */
public class SharedRedisPools {

    private static final Logger log = LoggerFactory.getLogger(SharedRedisPools.class);
    private static PoolProvider poolProvider;

    /*
     * As it is absolutely the default production behavior to use a standard redis pool provider
     * it is permissable to use a static initializer here.
     */
    static {
        log.info("Setting standard redis pool provider");
        setPoolProvider(new StandardRedisPoolProvider());
    }

    // For testing purposes, allows us to use a mocked provider so we can write tests without depending on redis
    public static void setPoolProvider(PoolProvider provider) {
        SharedRedisPools.poolProvider = provider;
    }

    public static PoolProvider getPoolProvider() {
        return poolProvider;
    }

    public static JedisPool getJedisPool(URI redisUri) {
        return poolProvider.getJedisPool(redisUri);
    }

    public static RedisPool getRedisPool(URI redisUri) {
        return poolProvider.getRedisPool(redisUri);
    }
}
