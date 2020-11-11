package org.gorpipe.spark;

import gorsat.process.PipeInstance;
import org.gorpipe.gor.util.ConfigUtil;
import org.gorpipe.gorshell.GorShell;
import org.gorpipe.gorshell.GorShellSessionFactory;
import org.gorpipe.logging.GorLogbackUtil;

import java.io.IOException;

public class GorSparkShell extends GorShell {

    public GorSparkShell(GorShellSessionFactory gorShellSessionFactory) {
        super(gorShellSessionFactory);
    }

    public static void main(String[] args) throws IOException {
        GorLogbackUtil.initLog("gorshell");

        ConfigUtil.loadConfig("gor");
        PipeInstance.initialize();

        GorShellSessionFactory gorShellSessionFactory = getSessionFactory();
        GorSparkShell gorShell = new GorSparkShell(gorShellSessionFactory);
        gorShell.run();
    }

    static GorShellSessionFactory getSessionFactory() {
        String cwd = System.getProperty("user.dir");
        return new GorSparkShellSessionFactory(cwd);
    }
}
