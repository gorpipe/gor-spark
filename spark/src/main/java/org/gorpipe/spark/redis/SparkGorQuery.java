package org.gorpipe.spark.redis;

import gorsat.process.GenericGorRunner;
import gorsat.process.PipeInstance;
import gorsat.process.SparkPipeInstance;
import org.gorpipe.gor.session.GorContext;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class SparkGorQuery implements Callable<List<String>> {
    GenericGorRunner genericGorRunner;
    String cmd;
    String cachefile;
    GorContext context;

    public SparkGorQuery(GorContext ctx, String cmd, String cachefile) {
        genericGorRunner = new GenericGorRunner();
        this.context = ctx;
        this.cmd = cmd;
        this.cachefile = cachefile;
    }

    @Override
    public List<String> call() throws Exception {
        SparkPipeInstance pi = new SparkPipeInstance(context, cachefile);
        pi.init(cmd, false, "");
        pi.theInputSource().pushdownWrite(cachefile);

        genericGorRunner.run(pi.getIterator(), pi.getPipeStep());
        return Collections.singletonList("a\tb\t"+cachefile);
    }
}