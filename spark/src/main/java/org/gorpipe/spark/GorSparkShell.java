package org.gorpipe.spark;

import gorsat.process.PipeInstance;
import org.gorpipe.gor.util.ConfigUtil;
import org.gorpipe.gorshell.GorShell;
import org.gorpipe.logging.GorLogbackUtil;

import java.io.IOException;

public class GorSparkShell extends GorShell {

    public GorSparkShell() throws IOException {
        super();
    }

    public static void main(String[] args) throws IOException {
        GorLogbackUtil.initLog("gorshell");

        ConfigUtil.loadConfig("gor");
        PipeInstance.initialize();

        GorSparkShell gorShell = new GorSparkShell();
        gorShell.run();
    }

    @Override
    protected void runQuery(String script) {
        resetRunner();
        runner = new SparkQueryRunner(script, lineReader, Thread.currentThread());
        initQueryRunner();
        runner.start();
    }
}
