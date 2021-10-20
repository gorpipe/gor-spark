package org.gorpipe.spark.platform;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@SuppressWarnings("javadoc")
public interface RedisTransaction extends Closeable {
    List<Object> exec();

    void hmset(String key, Map<String, String> map);

    void zadd(String rankedListKey, double calcBV, String key);

    void hset(String nameHashKey, String path, String key);

    void del(String key);

    void zrem(String rankedListKey, String key);

    void hdel(String nameHashKey, String path);

    void decrBy(String sizeKey, Long size);

    void incrBy(String sizeKey, Long size);

    void hincrBy(String key, String field, long value);
}
