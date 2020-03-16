package com.nextcode.gor.platform;

import redis.clients.jedis.JedisPool;


/**
 * RedisPool implementation wrapping real connections
 *
 * @author vilm
 */
public class JedisWrapperPool implements RedisPool {
    private JedisPool pool;

    /**
     * Create pool
     *
     * @param pool Jedis pool
     */
    public JedisWrapperPool(JedisPool pool) {
        this.pool = pool;
    }

    @Override
    public Redis getResource() {
        return new JedisWrapper(pool.getResource());
    }

    @Override
    public void destroy() {
        pool.destroy();
    }

    @Override
    public int inUse() {
        return pool.getNumActive();
    }

}
