package com.nextcode.gor.platform;

import org.apache.http.client.utils.URIBuilder;
import redis.clients.jedis.Protocol;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by gisli on 06/01/2017.
 */
public class JedisURIHelper {

    public static URI create(String uriStr) {
        if (uriStr == null || uriStr.length() == 0) {
            return null;
            //throw new IllegalArgumentException("Error creating URI, host is missing!");
        }
        try {
            URI candURI = URI.create("redis://" + stripSchema(uriStr));
            int db = getDBIndex(candURI);
            return new URIBuilder().setScheme("redis")
                    .setHost(candURI.getHost())
                    .setUserInfo(candURI.getUserInfo())
                    .setPort(candURI.getPort() > 0 ? candURI.getPort() : Protocol.DEFAULT_PORT)
                    .setPath("/" + (db > -1 ? db : Protocol.DEFAULT_DATABASE))
                    .build();
        } catch (URISyntaxException x) {
            throw new IllegalArgumentException(x.getMessage(), x);
        }
    }

    /**
     * Create redis Uri.
     *
     * @param host host
     * @param port port (if port >= 0 then the default port is used).
     * @param db   db (if db >= -1 then the default db is used).
     * @return uri representing the input parameters.
     */
    public static URI create(String host, int port, int db) {
        if (host == null || host.length() == 0) {
            return null;
            //throw new IllegalArgumentException("Error creating URI, host is missing!");
        }
        String uriStr = stripSchema(host) + ":" + (port > 0 ? port : Protocol.DEFAULT_PORT)
                + "/" + (db > -1 ? db : Protocol.DEFAULT_DATABASE);
        return create(uriStr);
    }

    @SuppressWarnings("unused")
    public static String getPassword(URI uri) {
        return redis.clients.jedis.util.JedisURIHelper.getPassword(uri);
    }

    public static int getDBIndex(URI uri) {
        return redis.clients.jedis.util.JedisURIHelper.getDBIndex(uri);
    }

    @SuppressWarnings("unused")
    public static boolean isValid(URI uri) {
        return redis.clients.jedis.util.JedisURIHelper.isValid(uri);
    }

    private static String stripSchema(String host) {
        return host.contains("://") ? host.substring(host.indexOf("://") + 3) : host;
    }
}
