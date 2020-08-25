package org.gorpipe.spark.platform;

import org.gorpipe.gor.monitor.GorMonitor;

/**
 * Forward messages from remote jobs to local job
 *
 * @author vilm
 */
public class GorMonitorGorLogForwarder implements GorLogReceiver {

    private GorMonitor mon;

    public GorMonitorGorLogForwarder(GorMonitor mon) {
        this.mon = mon;
    }

    @Override
    public void receiveLog(String channel, String jobId, String message) {
        mon.log(message);
    }

}
