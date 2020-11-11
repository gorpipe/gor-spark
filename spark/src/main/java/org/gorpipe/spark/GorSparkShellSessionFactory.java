package org.gorpipe.spark;

import gorsat.process.SessionBasedQueryEvaluator;
import org.gorpipe.client.FileCache;
import org.gorpipe.gor.model.DriverBackedFileReader;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.gor.session.ProjectContext;
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
    public ProjectContext initProjectContext(FileCache fileCache, GorSession session) {
        ProjectContext.Builder projectContextBuilder = new ProjectContext.Builder();
        return projectContextBuilder
                .setRoot(this.root)
                .setCacheDir(this.cacheDir)
                .setConfigFile(this.configFile)
                .setFileReader(new DriverBackedFileReader("", this.root, null))
                .setFileCache(fileCache)
                .setQueryHandler(new GeneralSparkQueryHandler((GorSparkSession)session, null))
                .setQueryEvaluator(new SessionBasedQueryEvaluator(session)).build();
    }
}
