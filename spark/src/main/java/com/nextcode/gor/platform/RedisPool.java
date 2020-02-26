package com.nextcode.gor.platform;


/**
 * Redis connection pool.
 * <p>
 * Pool interface that uses JedisCommands - used to make unit testing easier.
 *
 * @author vilm
 */
public interface RedisPool {

    /**
     * Get resource from pool.
     *
     * @return Redis connection
     */
    Redis getResource();

    /**
     * Destroy pool
     */
    void destroy();

    /**
     * @return Number of connections in use
     */
    int inUse();
}
