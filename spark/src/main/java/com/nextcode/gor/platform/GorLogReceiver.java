package com.nextcode.gor.platform;

/**
 * Receives private log messages from cluster jobs
 *
 * @author vilm
 */
public interface GorLogReceiver {
    /**
     * @param channel Channel received from
     * @param jobId   Job id
     * @param message Message
     */
    void receiveLog(String channel, String jobId, String message);
}
