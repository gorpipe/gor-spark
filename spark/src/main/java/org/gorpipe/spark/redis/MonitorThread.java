package org.gorpipe.spark.redis;

import org.gorpipe.exceptions.ExceptionUtilities;
import org.gorpipe.spark.platform.JedisURIHelper;
import org.gorpipe.spark.platform.SharedRedisPools;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

class MonitorThread implements Callable<String> {
    boolean running = true;
    private JedisPool jedisPool;
    private Map<String,Future<List<String>>> futureActionSet;

    public MonitorThread(String redisUri) {
        futureActionSet = new ConcurrentHashMap<>();
        if(redisUri!=null&&redisUri.length()>0) jedisPool = SharedRedisPools.getJedisPool(JedisURIHelper.create(redisUri));
    }

    public void stopRunning() {
        running = false;
    }

    public void addJob(String jobId, Future<List<String>> fut) {
        futureActionSet.put(jobId, fut);
    }

    private Duration getJobExpiration() {
        return Duration.ofMinutes(20);
    }

    public void setValue(String[] jobIds, String field, String value) {
        if(jedisPool!=null) try (Jedis jedis = jedisPool.getResource()) {
            for(String jobId : jobIds) {
                jedis.hset(jobId, field, value);
                jedis.expire(jobId, (int) getJobExpiration().getSeconds());
            }
        }
    }

    public void setValues(String[] jobIds, String field, String[] values) {
        if(jedisPool!=null) try (Jedis jedis = jedisPool.getResource()) {
            for(int i = 0; i < jobIds.length; i++) {
                String jobId = jobIds[i];
                String value = values[i];
                jedis.hset(jobId, field, value);
                jedis.expire(jobId, (int) getJobExpiration().getSeconds());
            }
        }
    }

    @Override
    public String call() throws Exception {
        try {
            String reskey = null;

            while(running) {
                for (String key : futureActionSet.keySet()) {
                    Future<List<String>> fut = futureActionSet.get(key);
                    String[] jobIds = key.split(",");
                    try {
                        List<String> res = fut.get(500, TimeUnit.MILLISECONDS);
                        reskey = key;
                        String[] cacheFiles = res.stream().map(s -> s.split("\t")).map(s -> s[2]).toArray(String[]::new);
                        setValues(jobIds, "result", cacheFiles);
                        setValue(jobIds, "status", "DONE");
                        break;
                    } catch (ExecutionException e) {
                        reskey = key;
                        setValue(jobIds, "error", ExceptionUtilities.gorExceptionToJson(e.getCause()));
                        setValue(jobIds, "status", "FAILED");
                        break;
                    } catch (TimeoutException e) {
                        // do nothing
                    }
                }

                if(reskey!=null) {
                    futureActionSet.remove(reskey);
                    reskey = null;
                }

                if(futureActionSet.isEmpty()) Thread.sleep(500);
            }
        } finally {
            if(jedisPool!=null) jedisPool.close();
        }
        return "";
    }
}