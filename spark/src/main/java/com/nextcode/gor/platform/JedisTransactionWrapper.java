package com.nextcode.gor.platform;

import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Map;

@SuppressWarnings("javadoc")
public class JedisTransactionWrapper implements RedisTransaction {

    private Transaction jedis;

    public JedisTransactionWrapper(Transaction trans) {
        this.jedis = trans;
    }


    Transaction jedis() {
        return jedis;
    }


    @Override
    public List<Object> exec() {
        return jedis.exec();
    }


    @Override
    public void close() {
        exec();
    }


    @Override
    public void hmset(String key, Map<String, String> map) {
        jedis.hmset(key, map);
    }


    @Override
    public void zadd(String key, double score, String member) {
        jedis.zadd(key, score, member);
    }


    @Override
    public void hset(String key, String field, String value) {
        jedis.hset(key, field, value);
    }


    @Override
    public void del(String key) {
        jedis.del(key);

    }


    @Override
    public void zrem(String key, String member) {
        jedis.zrem(key, member);
    }


    @Override
    public void hdel(String key, String field) {
        jedis.hdel(key, field);

    }

    @Override
    public void decrBy(String key, Long integer) {
        jedis.decrBy(key, integer);
    }


    @Override
    public void incrBy(String key, Long integer) {
        jedis.incrBy(key, integer);
    }

    @Override
    public void hincrBy(String key, String field, long value) {
        jedis.hincrBy(key, field, value);
    }
}
