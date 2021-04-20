package gorsat.spark;

import org.gorpipe.gor.model.GorParallelQueryHandler;
import org.gorpipe.gor.monitor.GorMonitor;

public class ReceiveQueryHandler implements GorParallelQueryHandler {
    String[] commandsToExecute;

    public String[] getCommandsToExecute() {
        return commandsToExecute;
    }

    @Override
    public String[] executeBatch(String[] fingerprints, String[] commandsToExecute, String[] batchGroupNames, String[] cacheFiles, GorMonitor cancelMonitor) {
        this.commandsToExecute = commandsToExecute;

        return new String[0];
    }

    @Override
    public void setForce(boolean force) {

    }

    @Override
    public void setQueryTime(Long time) {

    }

    @Override
    public long getWaitTime() {
        return 0;
    }
}
