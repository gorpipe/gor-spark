package org.gorpipe.spark.platform;

import org.slf4j.MDC;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles subscription to job log channels.
 * Important to close to free up redis connection.
 *
 * @author vilm
 */
public class RedisLogSubscription extends JedisPubSub implements GorLogSubscription {
    private GorClusterBase cluster;
    private GorLogReceiver receiver;
    private String[] channels;
    private HashMap<String, String> chanToId = new HashMap<>();
    private Thread subscriptionThread;
    private Thread unsubscriptionThread;

    public RedisLogSubscription(GorClusterBase cluster, GorLogReceiver receiver, String[] jobIds, String key) {
        this.cluster = cluster;
        this.receiver = receiver;
        this.channels = new String[jobIds.length];
        for (int i = 0; i < jobIds.length; i++) {
            channels[i] = getPrivateLogKey(key, jobIds[i]);
            chanToId.put(channels[i], jobIds[i]);
        }
    }

    public static String getKey(String... parts) {
        return String.join(":", parts);
    }

    public static String getPrivateLogKey(String key, String jobId) {
        return getKey(key, "DC", "JOB", jobId, "LOG");
    }

    @Override
    public void start() {
        Map<String,String> mdcContext = MDC.getCopyOfContextMap();
        unsubscriptionThread = new Thread("LogSubscription - Unsubscripion thread") {

            @Override
            public void run() {
                if(mdcContext != null) MDC.setContextMap(mdcContext);

                try {
                    Thread.sleep(cluster.getJobRetention().toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                cluster.logWarn("Subscription timeout", null);
                unsubscribe();
            }
        };
        unsubscriptionThread.start();
        subscriptionThread = new Thread("LogSubscription - subscription thread") {
            @Override
            public void run() {
                if(mdcContext != null) MDC.setContextMap(mdcContext);
                cluster.logInfo("Starting log subscription on channels " + Arrays.toString(channels), null);
                try (Jedis j = cluster.pool().getResource()) {
                    j.subscribe(RedisLogSubscription.this, channels);
                } catch (JedisConnectionException jde) {
                    // as redis is only for the progress bar in spark environment, ignore errors and allow job to finish
                    // throw new GorSystemException("Unable to get a connection to redis at " + cluster.getConfig().getURI(), jde);
                } finally {
                    cluster.logInfo("Ending log subscription on channels " + Arrays.toString(channels), null);
                    unsubscriptionThread.interrupt();
                }
            }
        };
        subscriptionThread.start();
    }

    @Override
    public void close() {
        cluster.logInfo("Calling unsubscribe", null);
        unsubscribe();
    }

    @Override
    public void onMessage(String channel, String message) {
        receiver.receiveLog(channel, chanToId.get(channel), message);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
        receiver.receiveLog(channel, chanToId.get(channel), message);
    }

    @Override
    public void onPSubscribe(String arg0, int arg1) {/**/}

    @Override
    public void onPUnsubscribe(String arg0, int arg1) {/**/}

    @Override
    public void onSubscribe(String arg0, int arg1) {/**/}

    @Override
    public void onUnsubscribe(String arg0, int arg1) {/**/}

}
