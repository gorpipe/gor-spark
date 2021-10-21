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


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

/**
 * Factory class for creating default/generic gor sessions. This factory replaces the createDefaultSession() method.
 */
public class SparkSessionFactory extends GorSessionFactory {

    private String root;
    private String cacheDir;
    private Optional<String> configFile;
    private Optional<String> aliasFile;
    private SparkSession sparkSession;
    private GorMonitor sparkGorMonitor;
    private GorParallelQueryHandler sparkQueryHandler;
    private String securityContext;
    private int workers;

    public SparkSessionFactory(SparkSession sparkSession, String root, String cacheDir, String configFile, String aliasFile, String securityContext, GorMonitor sparkMonitor, int workers) {
        this.root = root;
        this.cacheDir = cacheDir;
        Path rootPath = Paths.get(root);
        if(configFile != null && configFile.length() > 0) {
            Path configPath = Paths.get(configFile);
            if(!configPath.isAbsolute()) configPath = rootPath.resolve(configPath);
            this.configFile = Files.exists(configPath) ? Optional.of(configPath.toAbsolutePath().toString()) : Optional.empty();
        } else this.configFile = Optional.empty();
        if(aliasFile != null && aliasFile.length() > 0) {
            Path aliasPath = Paths.get(aliasFile);
            if(!aliasPath.isAbsolute()) aliasPath = rootPath.resolve(aliasPath);
            this.aliasFile = Files.exists(aliasPath) ? Optional.of(aliasPath.toAbsolutePath().toString()) : Optional.empty();
        } else this.aliasFile = Optional.empty();
        this.sparkSession = sparkSession;
        this.sparkGorMonitor = sparkMonitor;
        this.workers = workers;
        this.securityContext = securityContext;
    }

    public SparkSessionFactory(SparkSession sparkSession, String root, String cacheDir, String configFile, String aliasFile, String securityContext, GorMonitor sparkMonitor) {
        this(sparkSession, root, cacheDir, configFile, aliasFile, securityContext, sparkMonitor, new GeneralSparkQueryHandler());
    }

    public SparkSessionFactory(SparkSession sparkSession, String root, String cacheDir, String configFile, String aliasFile, String securityContext, GorMonitor sparkMonitor, GorParallelQueryHandler queryHandler) {
        this(sparkSession, root, cacheDir, configFile, aliasFile, securityContext, sparkMonitor, 0);
        this.sparkQueryHandler = queryHandler;
    }

    public GorSparkSession generateSession() {
        String requestId = UUID.randomUUID().toString();
        GorSparkSession session = new GorSparkSession(requestId, workers);
        session.setSparkSession(sparkSession);
        return session;
    }

    @Override
    public GorSession create() {
        GorSparkSession session;
        if(sparkQueryHandler instanceof GeneralSparkQueryHandler generalSparkQueryHandler) {
            if(generalSparkQueryHandler.gpSession!=null) {
                session = generalSparkQueryHandler.gpSession;
            } else {
                session = generateSession();
                generalSparkQueryHandler.init(session);
            }
        } else {
            session = generateSession();
        }
        Path cachePath = Paths.get(cacheDir);

        ProjectContext.Builder projectContextBuilder = new ProjectContext.Builder();
        if(configFile.isPresent()) projectContextBuilder = projectContextBuilder.setConfigFile(configFile.get());
        if(aliasFile.isPresent()) projectContextBuilder = projectContextBuilder.setAliasFile(aliasFile.get());
        projectContextBuilder = projectContextBuilder
            .setRoot(root)
            .setCacheDir(cacheDir)
            .setFileReader(new DriverBackedFileReader(securityContext, root, null))
            .setFileCache(new LocalFileCacheClient(cachePath.isAbsolute() ? cachePath : Paths.get(root).resolve(cacheDir)))
            .setQueryHandler(sparkQueryHandler)
            .setQueryEvaluator(new SessionBasedQueryEvaluator(session));

        SystemContext.Builder systemContextBuilder = new SystemContext.Builder();
        systemContextBuilder
                .setReportBuilder(new FreemarkerReportBuilder(session))
                .setRunnerFactory(new GenericRunnerFactory())
                .setServer(false)
                .setMonitor(sparkGorMonitor)
                .setStartTime(System.currentTimeMillis());

        GorSessionCache cache = GorSessionCacheManager.getCache(session.getRequestId());

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
