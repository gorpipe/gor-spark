package com.nextcode.gor.spark;

import com.nextcode.spark.SparkGORSession;
import gorsat.process.FreemarkerReportBuilder;
import gorsat.process.GenericRunnerFactory;
import gorsat.process.SessionBasedQueryEvaluator;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.*;
import org.gorpipe.gor.clients.LocalFileCacheClient;
import org.gorpipe.model.genome.files.gor.DriverBackedFileReader;
import org.gorpipe.model.genome.files.gor.GorMonitor;

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

    public SparkSessionFactory(String root, String cacheDir, SparkGorMonitor sparkMonitor) {
       this(SparkGorUtilities.getSparkSession(root,""), root, cacheDir, sparkMonitor);
    }

    public SparkSessionFactory(SparkSession sparkSession, String root, String cacheDir, GorMonitor sparkMonitor) {
        this.root = root;
        this.cacheDir = cacheDir;
        this.sparkSession = sparkSession;
        this.sparkGorMonitor = sparkMonitor;
    }

    @Override
    public GorSession create() {
        String requestId = UUID.randomUUID().toString();
        SparkGORSession session = new SparkGORSession(requestId);
        session.setSparkSession(sparkSession);
        if(sparkGorMonitor instanceof SparkGorMonitor) {
            session.redisUri_$eq(((SparkGorMonitor)sparkGorMonitor).getRedisUri());
        }

        Path cachePath = Paths.get(cacheDir);

        GeneralSparkQueryHandler sparkQueryHandler = new GeneralSparkQueryHandler(null);
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
        sparkQueryHandler.init(session);

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
