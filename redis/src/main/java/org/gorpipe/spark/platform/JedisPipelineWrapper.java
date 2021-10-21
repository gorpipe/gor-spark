package org.gorpipe.spark.platform;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Wrap jedis pipeline
 *
 * @author vilm
 */
public class JedisPipelineWrapper implements RedisPipeline {

    private Pipeline pipeline;
    private List<Response<?>> responses = new ArrayList<>();
    private Jedis jedis;

    /**
     * Create jedis pipeline wrapper
     *
     * @param jedis     jedis
     * @param pipelined pipeline
     */
    public JedisPipelineWrapper(Jedis jedis, Pipeline pipelined) {
        this.jedis = jedis;
        this.pipeline = pipelined;
    }

    @Override
    public void close() {
        sync();
        jedis.ping();
    }

    @Override
    public void sync() {
        pipeline.sync();
    }

    @Override
    public void hmset(String key, Map<String, String> map) {
        responses.add(pipeline.hmset(key, map));
    }


    @Override
    public void zadd(String key, double score, String member) {
        responses.add(pipeline.zadd(key, score, member));
    }


    @Override
    public void hset(String key, String field, String value) {
        responses.add(pipeline.hset(key, field, value));
    }


    @Override
    public void del(String key) {
        responses.add(pipeline.del(key));

    }


    @Override
    public void zrem(String key, String member) {
        responses.add(pipeline.zrem(key, member));
    }


    @Override
    public void hdel(String key, String field) {
        responses.add(pipeline.hdel(key, field));
    }

    @Override
    public void decrBy(String key, Long integer) {
        responses.add(pipeline.decrBy(key, integer));
    }


    @Override
    public void incrBy(String key, Long integer) {
        responses.add(pipeline.incrBy(key, integer));
    }

    @Override
    public List<Response<?>> getResponses() {
        return responses;
    }

    @Override
    public void hgetall(String string) {
        responses.add(pipeline.hgetAll(string));
    }


}
