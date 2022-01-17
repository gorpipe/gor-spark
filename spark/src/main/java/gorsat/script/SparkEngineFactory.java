package gorsat.script;

import gorsat.QueryHandlers.QueryHandler;
import gorsat.Script.ScriptExecutionEngine;
import org.gorpipe.gor.session.GorContext;
import org.gorpipe.spark.SparkTableQueryHandler;

public class SparkEngineFactory {
    public static ScriptExecutionEngine create(GorContext context) {
        var remoteQueryHandler = context.getSession().getProjectContext().getQueryHandler();
        if (remoteQueryHandler == null) {
            remoteQueryHandler = new QueryHandler(context);
        }

        var localQueryHandler = new SparkTableQueryHandler(context, false);
        return new ScriptExecutionEngine(remoteQueryHandler, localQueryHandler, context);
    }
}
