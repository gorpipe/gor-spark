package org.gorpipe.spark;

import org.gorpipe.gor.model.GorParallelQueryHandler;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.gorshell.GorShellSessionFactory;

import java.util.UUID;

public class GorSparkShellSessionFactory extends GorShellSessionFactory {
    public GorSparkShellSessionFactory(String root) {
        super(root);
    }

    @Override
    protected GorSession createSession() {
        String requestId = UUID.randomUUID().toString();
        return new GorSparkSession(requestId);
    }

    @Override
    protected GorParallelQueryHandler createQueryHandler(GorSession session) {
        return new GeneralSparkQueryHandler((GorSparkSession)session, null);
    }
}
