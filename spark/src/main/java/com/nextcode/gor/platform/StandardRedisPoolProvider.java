package com.nextcode.gor.platform;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

public class StandardRedisPoolProvider implements PoolProvider {

    private static final int DEFAULT_TIMEOUT_MS = 30000; // 30 seconds
    private static final int SOCKET_TIMEOUT_MS = 5000; // 5 seconds
    private static final Logger log = LoggerFactory.getLogger(StandardRedisPoolProvider.class);
    private final ConcurrentHashMap<URI, JedisPool> pools = new ConcurrentHashMap<>();

    private JedisPool createPool(URI redisUri) {

        log.info("Creating Shared Jedis Pool for '{}'", redisUri);
        // Configuration settings improved using guide from https://gist.github.com/JonCole/925630df72be1351b21440625ff2671f
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

        //noinspection MagicNumber
        poolConfig.setMaxTotal(1024);
        //noinspection MagicNumber
        poolConfig.setMaxIdle(256);
        //noinspection MagicNumber
        poolConfig.setMinIdle(128);

        // Block threads attempting to get a connection from the pool when the pool is exhausted
        // for up to 2 minutes until giving up.
        poolConfig.setBlockWhenExhausted(true);
        //noinspection MagicNumber
        poolConfig.setMaxWaitMillis(2 * 60 * 1000);

        // Every 30 seconds - evict connections idle more than 2 minutes.
        //noinspection MagicNumber
        poolConfig.setTimeBetweenEvictionRunsMillis(30 * 1000);
        //noinspection MagicNumber
        poolConfig.setMinEvictableIdleTimeMillis(2 * 60 * 1000);


        // Ensure that pool serves only good connections
        poolConfig.setTestOnBorrow(true);

        return new RetryConnectionJedisPool(poolConfig, redisUri, DEFAULT_TIMEOUT_MS, SOCKET_TIMEOUT_MS);
    }

    @Override
    public JedisPool getJedisPool(URI redisUri) {
        return pools.computeIfAbsent(redisUri, this::createPool);
    }

    // TODO - Remove and removed mocked Redis Pool since we no longer need that pattern with Mockito
    // TODO - and the pool provider being replaceable
    @Override
    public RedisPool getRedisPool(URI redisUri) {
        return new JedisWrapperPool(getJedisPool(redisUri));
    }
}
