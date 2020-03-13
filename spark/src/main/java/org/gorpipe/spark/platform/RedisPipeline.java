package com.nextcode.gor.platform;

import redis.clients.jedis.Response;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@SuppressWarnings("javadoc")
public interface RedisPipeline extends Closeable {

    void hmset(String key, Map<String, String> map);

    void incrBy(String key, Long integer);

    void decrBy(String key, Long integer);

    void hdel(String key, String field);

    void zrem(String key, String member);

    void del(String key);

    void hset(String key, String field, String value);

    void zadd(String key, double score, String member);

    void sync();

    List<Response<?>> getResponses();

    void hgetall(String string);

}
