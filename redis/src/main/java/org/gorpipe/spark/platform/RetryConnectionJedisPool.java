package org.gorpipe.spark.platform;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;

class RetryConnectionJedisPool extends JedisPool {

    private static final Logger log = LoggerFactory.getLogger(RetryConnectionJedisPool.class);

    RetryConnectionJedisPool(GenericObjectPoolConfig poolConfig, URI redisUri, int defaultTimeout, int socketTimeout) {
        super(poolConfig, redisUri, defaultTimeout, socketTimeout);
    }

    /**
     * Attempts to get a resource from the pool, handling connection exceptions up to 5 times
     * @return a valid connection to redis or null if retry logic was not able to create a valid connection
     */
    @Override
    public Jedis getResource() {
        Jedis jedis;
        int numRetries = 0;
        while (numRetries <= 5) {
            try {
                jedis = super.getResource();
                return jedis;
            } catch (Exception e) {
                if (numRetries >= 5) {
                    throw e;
                }
                numRetries++;
                log.info("Got an exception {}, retrying connection in {} ms", e, numRetries * 500);
                try {
                    Thread.sleep(numRetries * 500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    //noinspection BreakStatement
                    break;
                }
            }
        }
        return null;
    }

}