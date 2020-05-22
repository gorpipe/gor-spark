package gorsat.spark;

import org.gorpipe.model.genome.files.gor.GorMonitor;
import org.gorpipe.model.genome.files.gor.GorParallelQueryHandler;

public class ReceiveQueryHandler implements GorParallelQueryHandler {
    String[] commandsToExecute;

    public String[] getCommandsToExecute() {
        return commandsToExecute;
    }

    @Override
    public String[] executeBatch(String[] fingerprints, String[] commandsToExecute, String[] batchGroupNames, GorMonitor cancelMonitor) {
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
