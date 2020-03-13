package org.gorpipe.spark.platform;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Wrapper class delegating to real jedis connection
 *
 * @author vilm
 */
public class JedisWrapper implements Redis, Closeable {

    private Jedis jedis;

    /**
     * Wrap jedis connection and pool
     *
     * @param jedis jedis connection
     */
    JedisWrapper(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public String get(String key) {
        return jedis.get(key);
    }

    @Override
    public String getSet(String key, String value) {
        return jedis.getSet(key, value);
    }

    @Override
    public String set(String key, String value) {
        return jedis.set(key, value);
    }

    @Override
    public Long setnx(String key, String value) {
        return jedis.setnx(key, value);
    }


    @Override
    public Long del(String... keys) {
        return jedis.del(keys);
    }

    @Override
    public Long incr(String key) {
        return jedis.incr(key);
    }

    @Override
    public String hget(String key, String field) {
        return jedis.hget(key, field);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return jedis.hset(key, field, value);
    }

    @Override
    public Long expire(String key, int seconds) {
        return jedis.expire(key, seconds);
    }

    @Override
    public Long publish(String channel, String message) {
        return jedis.publish(channel, message);

    }

    @Override
    public boolean exists(String key) {
        return jedis.exists(key);
    }

    @Override
    public void close() {
        jedis.close();
    }

    @Override
    public Long zadd(String key, double score, String val) {
        return jedis.zadd(key, score, val);
    }

    @Override
    public Long zrem(String key, String... members) {
        return jedis.zrem(key, members);

    }

    @Override
    public Long hdel(String key, String... fields) {
        return jedis.hdel(key, fields);
    }

    @Override
    public Long zcard(String key) {
        return jedis.zcard(key);
    }

    @Override
    public boolean hexists(String key, String field) {
        return jedis.hexists(key, field);
    }

    @Override
    public Set<String> zrange(String key, long start, long stop) {
        return jedis.zrange(key, start, stop);
    }

    @Override
    public Long incrBy(String key, long value) {
        return jedis.incrBy(key, value);
    }

    @Override
    public Long decrBy(String key, long value) {
        return jedis.decrBy(key, value);
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return jedis.hincrBy(key, field, value);
    }

    @Override
    public Map<String, String> hgetall(String key) {
        return jedis.hgetAll(key);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return jedis.hmget(key, fields);
    }

    @Override
    public void hmset(String key, Map<String, String> data) {
        jedis.hmset(key, data);
    }

    @Override
    public Set<String> keys(String pat) {
        return jedis.keys(pat);
    }

    @Override
    public RedisTransaction multi() {
        Transaction trans = jedis.multi();
        return new JedisTransactionWrapper(trans);
    }

    @Override
    public RedisPipeline pipelined() {
        return new JedisPipelineWrapper(jedis, jedis.pipelined());
    }

    @Override
    public String ping() {
        return jedis.ping();
    }
}
