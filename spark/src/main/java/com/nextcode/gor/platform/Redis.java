package com.nextcode.gor.platform;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines subset of Redis commands.  Makes unit testing easier.
 * <p>
 * See redis documentation.
 *
 * @author vilm
 */
@SuppressWarnings("javadoc")
public interface Redis extends Closeable {
    Long del(String... keys);

    String get(String key);

    String getSet(String key, String value);

    Long setnx(String key, String value);

    String set(String key, String value);

    Long incr(String key);

    String hget(String key, String field);

    Long hset(String key, String field, String value);

    Long expire(String key, int seconds);

    Long publish(String channel, String message);

    boolean exists(String key);

    Long zadd(String key, double score, String val);

    Long zrem(String key, String... members);

    Long hdel(String key, String... fields);

    Long zcard(String key);

    boolean hexists(String key, String field);

    Set<String> zrange(String key, long start, long stop);

    Long incrBy(String sizeKey, long size);

    Long decrBy(String sizeKey, long size);

    Long hincrBy(String key, String field, long value);

    Map<String, String> hgetall(String key);

    List<String> hmget(String key, String... fields);

    void hmset(String key, Map<String, String> data);

    Set<String> keys(String pat);

    RedisTransaction multi();

    @Override
    void close();

    RedisPipeline pipelined();

    String ping();
}
