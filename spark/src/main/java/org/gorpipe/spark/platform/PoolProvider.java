package org.gorpipe.spark.platform;

import redis.clients.jedis.JedisPool;
import java.net.URI;

public interface PoolProvider {
    @SuppressWarnings("javadoc")
    JedisPool getJedisPool(URI redisUri);

    @SuppressWarnings("javadoc")
    RedisPool getRedisPool(URI redisUri);
}