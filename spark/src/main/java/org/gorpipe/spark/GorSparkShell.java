package org.gorpipe.spark;

import gorsat.process.PipeInstance;
import org.gorpipe.gor.util.ConfigUtil;
import org.gorpipe.gorshell.GorShell;
import org.gorpipe.gorshell.GorShellSessionFactory;
import org.gorpipe.gorshell.QueryRunner;
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
        String cwd = System.getProperty("user.dir");
        GorShellSessionFactory sessionFactory = new GorSparkShellSessionFactory(cwd);
        runner = new QueryRunner(sessionFactory, script, lineReader, Thread.currentThread());
        initQueryRunner();
        runner.start();
    }
}
