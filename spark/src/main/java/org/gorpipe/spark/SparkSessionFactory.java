package org.gorpipe.spark;

import gorsat.process.*;
import org.apache.spark.sql.SparkSession;

import org.gorpipe.gor.clients.LocalFileCacheClient;
import org.gorpipe.gor.model.DriverBackedFileReader;
import org.gorpipe.gor.model.GorParallelQueryHandler;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.gor.session.GorSessionCache;
import org.gorpipe.gor.session.ProjectContext;
import org.gorpipe.gor.session.SystemContext;


import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Factory class for creating default/generic gor sessions. This factory replaces the createDefaultSession() method.
 */
public class SparkSessionFactory extends GorSessionFactory {

    private String root;
    private String cacheDir;
    private SparkSession sparkSession;
    private GorMonitor sparkGorMonitor;
    private GorParallelQueryHandler queryHandler;

    public SparkSessionFactory(String root, String cacheDir, SparkGorMonitor sparkMonitor) {
       this(GorSparkUtilities.getSparkSession(), root, cacheDir, sparkMonitor);
    }

    public SparkSessionFactory(SparkSession sparkSession, String root, String cacheDir, GorMonitor sparkMonitor) {
        this.root = root;
        this.cacheDir = cacheDir;
        this.sparkSession = sparkSession;
        this.sparkGorMonitor = sparkMonitor;
    }

    public SparkSessionFactory(SparkSession sparkSession, String root, String cacheDir, GorMonitor sparkMonitor, GorParallelQueryHandler queryHandler) {
        this(sparkSession, root, cacheDir, sparkMonitor);
        this.queryHandler = queryHandler;
    }

    @Override
    public GorSession create() {
        String requestId = UUID.randomUUID().toString();
        GorSparkSession session = new GorSparkSession(requestId);
        session.setSparkSession(sparkSession);
        String sparkRedisUri = null;
        if(sparkGorMonitor instanceof SparkGorMonitor) {
            sparkRedisUri = ((SparkGorMonitor)sparkGorMonitor).getRedisUri();
            session.redisUri_$eq(sparkRedisUri);
        }

        Path cachePath = Paths.get(cacheDir);

        GorParallelQueryHandler sparkQueryHandler = queryHandler != null ? queryHandler : new GeneralSparkQueryHandler(null, sparkRedisUri);
        ProjectContext.Builder projectContextBuilder = new ProjectContext.Builder();
        projectContextBuilder
            .setRoot(root)
            .setCacheDir(cacheDir)
            .setFileReader(new DriverBackedFileReader("", root, null))
            .setFileCache(new LocalFileCacheClient(cachePath.isAbsolute() ? cachePath : Paths.get(root).resolve(cacheDir)))
            .setQueryHandler(sparkQueryHandler)
            .setQueryEvaluator(new SessionBasedQueryEvaluator(session)).build();

        SystemContext.Builder systemContextBuilder = new SystemContext.Builder();
        systemContextBuilder
                .setReportBuilder(new FreemarkerReportBuilder(session))
                .setRunnerFactory(new GenericRunnerFactory())
                .setServer(false)
                .setMonitor(sparkGorMonitor)
                .setStartTime(System.currentTimeMillis());

        GorSessionCache cache = GorSessionCacheManager.getCache(requestId);

        session.init(projectContextBuilder.build(),
                systemContextBuilder.build(),
                cache);
        if(sparkQueryHandler instanceof GeneralSparkQueryHandler) ((GeneralSparkQueryHandler)sparkQueryHandler).init(session);

        //session.redisUri_$eq(redisUri);

        /*SparkQueryHandler sqh = new SparkQueryHandler(session);

        GorContext builder = new GorContext(session);
        session.gorContext_$eq(builder.setFileReader(new DriverBackedFileReader("", root, null))
                .setFileCache(new LocalFileCacheClient(Paths.get(AnalysisUtilities.theCacheDirectory(session))))
                .setQueryHandler(new GeneralQueryHandler(session, false))
                .setSparkQueryHandler(sqh)
                .setQueryEvaluator(new SessionBasedQueryEvaluator(session))
                .setRunnerFactory(new GenericRunnerFactory())
                .build());

        AnalysisUtilities.loadAndSetConfig(session.gorConfigFile(), session);*/

        return session;
    }
}
