package org.gorpipe.spark;

import gorsat.QueryHandlers.GeneralQueryHandler;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorContext;

public class SparkTableQueryHandler extends GeneralQueryHandler {
    GorSparkSession session;

    public SparkTableQueryHandler(GorContext context, boolean header) {
        super(context, header);
        session = (GorSparkSession)context.getSession();
    }

    @Override
    public String[] executeBatch(String[] commandSignatures, String[] commandsToExecute, String[] batchGroupNames, String[] cacheFiles, GorMonitor gorMonitor) {
        var ret = super.executeBatch(commandSignatures, commandsToExecute, batchGroupNames, cacheFiles, gorMonitor);
        for(int k = 0; k < ret.length; k++) {
            var fileName = ret[k];
            if (fileName!=null) {
                var viewName = batchGroupNames[k];
                var tableName = viewName.substring(1, viewName.length() - 1);
                var fileNameLower = fileName.toLowerCase();
                if (fileNameLower.endsWith(".gor") || fileNameLower.endsWith(".gorz") || fileNameLower.endsWith(".gord")) {
                    session.dataframe("pgor " + fileName, null).createOrReplaceTempView(tableName);
                } else {
                    session.dataframe("nor " + fileName, null).createOrReplaceTempView(tableName);
                }
            }
        }
        return ret;
    }
}
