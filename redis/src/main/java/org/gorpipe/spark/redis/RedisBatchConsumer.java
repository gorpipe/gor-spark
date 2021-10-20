package org.gorpipe.spark.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import gorsat.Commands.CommandParseUtilities;
import gorsat.process.FreemarkerReportBuilder;
import gorsat.process.GenericRunnerFactory;
import gorsat.process.GorSessionCacheManager;
import gorsat.process.PipeOptions;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.clients.LocalFileCacheClient;
import org.gorpipe.gor.model.DriverBackedFileReader;
import org.gorpipe.gor.session.GorContext;
import org.gorpipe.gor.session.GorSessionCache;
import org.gorpipe.gor.session.ProjectContext;
import org.gorpipe.gor.session.SystemContext;
import org.gorpipe.spark.GeneralSparkQueryHandler;
import org.gorpipe.spark.GorQueryRDD;
import org.gorpipe.spark.GorSparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RedisBatchConsumer implements VoidFunction2<Dataset<Row>, Long>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RedisBatchConsumer.class);
    private static final String DEFAULT_CACHE_DIR = "result_cache";

    GorSparkSession gss;
    SystemContext sysctx;
    MonitorThread mont;
    ExecutorService es;

    public RedisBatchConsumer(SparkSession sparkSession, String redisUri) {
        log.info("Starting RedisBatchConsumer on redisUri "+ redisUri);

        gss = new GorSparkSession("", 0);
        gss.setSparkSession(sparkSession);
        gss.redisUri_$eq(redisUri);

        SystemContext.Builder systemContextBuilder = new SystemContext.Builder();
        sysctx = systemContextBuilder
                .setReportBuilder(new FreemarkerReportBuilder(gss))
                .setRunnerFactory(new GenericRunnerFactory())
                .setServer(true)
                .setStartTime(System.currentTimeMillis())
                .build();

        es = Executors.newWorkStealingPool(4);
        log.info("Starting monitorthread");
        mont = new MonitorThread(redisUri);
        es.submit(mont);
        log.info("Monitorthread submitted");
    }

    @Override
    public void close() {
        log.info("Closing RedisBatchConsumer");

        mont.stopRunning();
        es.shutdown();
    }

    public Future<List<String>> runGorJobs(String projectDirStr, Set<Integer> gorJobs, String[] queries, String[] fingerprints, String[] jobIds, String[] cachefiles, String[] secCtxs) {
        String[] newCommands = new String[gorJobs.size()];
        String[] newFingerprints = new String[gorJobs.size()];
        String[] newCacheFiles = new String[gorJobs.size()];
        String[] newJobIds = new String[gorJobs.size()];
        String[] newSecCtxs = new String[gorJobs.size()];

        int k = 0;
        for (int i : gorJobs) {
            newCommands[k] = queries[i];
            newFingerprints[k] = fingerprints[i];
            newJobIds[k] = jobIds[i].substring(jobIds[i].lastIndexOf(':')+1);
            newCacheFiles[k] = cachefiles[i];
            newSecCtxs[k] = secCtxs[i];
            k++;
        }
        String configFile = gss.getProjectContext() != null ? gss.getProjectContext().getGorConfigFile() : null;
        String aliasFile = gss.getProjectContext() != null ? gss.getProjectContext().getGorAliasFile() : null;
        GorQueryRDD gorQueryRDD = new GorQueryRDD(gss.sparkSession(), newCommands, newFingerprints, newCacheFiles, projectDirStr, "result_cache", configFile, aliasFile, newJobIds, newSecCtxs, gss.redisUri());
        return gorQueryRDD.toJavaRDD().collectAsync();
    }

    public Future<List<String>> runSparkJob(String projectDirStr,String[] creates,String cmd,String jobId,String cacheFile,String securityContext) {
        log.info("Running spark job "+jobId+": " + cmd);
        String shortJobId = jobId.substring(jobId.lastIndexOf(':')+1);

        String cacheDir = DEFAULT_CACHE_DIR;
        String configFile = System.getProperty("gor.project.config.path","config/gor_config.txt");
        String aliasFile = System.getProperty("gor.project.alias.path","config/gor_standard_aliases.txt");
        Path projectPath = Paths.get(projectDirStr);
        configFile = projectPath.resolve(configFile).toAbsolutePath().normalize().toString();
        aliasFile = projectPath.resolve(aliasFile).toAbsolutePath().normalize().toString();

        GeneralSparkQueryHandler queryHandler = new GeneralSparkQueryHandler(gss, gss.redisUri());

        ProjectContext.Builder projectContextBuilder = new ProjectContext.Builder();
        ProjectContext prjctx = projectContextBuilder
                .setRoot(projectDirStr+securityContext)
                .setCacheDir(cacheDir)
                .setFileReader(new DriverBackedFileReader(securityContext, projectDirStr, null))
                .setConfigFile(configFile)
                .setAliasFile(aliasFile)
                .setQueryHandler(queryHandler)
                .setFileCache(new LocalFileCacheClient(projectPath.resolve(cacheDir)))
                .build();

        GorSessionCache cache = GorSessionCacheManager.getCache(gss.getRequestId());
        gss.init(prjctx, sysctx, cache);
        GorContext context = new GorContext(gss);

        int firstSpace = cmd.indexOf(' ');
        cmd = cmd.substring(0, firstSpace + 1) + "-j " + shortJobId + cmd.substring(firstSpace);
        String query = creates.length > 0 ? String.join(";",creates) + ";" + cmd : cmd;
        cmd = gss.replaceAliases(query);
        String[] args = new String[]{cmd, "-queryhandler", "spark"};
        PipeOptions options = new PipeOptions();
        options.parseOptions(args);

        SparkGorQuery sgq = new SparkGorQuery(context, cmd, cacheFile);
        return es.submit(sgq);
    }

    public Map<String,Future<List<String>>> runJobBatch(List<String[]> lstr) {
        Optional<String> projectDir = lstr.stream().map(l -> l[2]).findFirst();
        Map<String,Future<List<String>>> futList = new HashMap<>();
        if (projectDir.isPresent()) {
            String projectDirStr = projectDir.get();
            String[] queries = lstr.stream().map(l -> l[0]).toArray(String[]::new);
            String[] fingerprints = lstr.stream().map(l -> l[1]).toArray(String[]::new);
            String[] cachefiles = lstr.stream().map(l -> l[5]).toArray(String[]::new);
            String[] jobIds = lstr.stream().map(l -> l[4]).toArray(String[]::new);
            String[] securityCtxs = lstr.stream().map(l -> l[6]).toArray(String[]::new);

            mont.setValue(jobIds, "status", "RUNNING");

            final Set<Integer> gorJobs = new TreeSet<>();
            for (int i = 0; i < queries.length; i++) {
                String cmd = queries[i];
                String jobId = jobIds[i];
                String cachefile = cachefiles[i];
                String securityContext = securityCtxs[i];
                String[] cmdSplit = CommandParseUtilities.quoteSafeSplit(cmd,';');
                String lastCommand = cmdSplit[cmdSplit.length-1].trim();
                String lastCommandUpper = lastCommand.toUpperCase();
                if (lastCommandUpper.startsWith("SELECT ") || lastCommandUpper.startsWith("SPARK ") || lastCommandUpper.startsWith("GORSPARK ") || lastCommandUpper.startsWith("NORSPARK ")) {
                    Future<List<String>> fut = runSparkJob(projectDirStr, Arrays.copyOfRange(cmdSplit,0,cmdSplit.length-1), lastCommand, jobId, cachefile, securityContext);
                    futList.put(jobId,fut);
                } else {
                    gorJobs.add(i);
                }
            }

            if (gorJobs.size() > 0) {
                Future<List<String>> fut = runGorJobs(projectDirStr, gorJobs, queries, fingerprints, jobIds, cachefiles, securityCtxs);
                String jobIdStr = gorJobs.stream().map(i -> jobIds[i]).collect(Collectors.joining(","));
                futList.put(jobIdStr,fut);
            }
        }
        return futList;
    }

    @Override
    public void call(Dataset<Row> v1, Long v2) {
        List<Row> rr = v1.collectAsList();
        log.info("Received batch of " + rr.size());
        List<String[]> lstr = rr.stream().filter(r -> r.getString(2).equals("payload")).map(r -> {
            String jobid = r.getString(1);
            String value = r.getString(3);
            String securityContext = r.getString(4);
            ObjectMapper om = new ObjectMapper();
            try {
                String mvalue = value.substring(1, value.length() - 1);
                Map<String, String> map = om.readValue(mvalue, Map.class);
                String gorquerybase = map.get("query");
                String fingerprint = map.get("fingerprint");
                String projectRoot = map.get("projectRoot");
                String requestId = map.get("request-id");
                String gorquery = new String(Base64.getDecoder().decode(gorquerybase));
                String cachefile = "result_cache/" + fingerprint + CommandParseUtilities.getExtensionForQuery(gorquery, false);
                if (map.containsKey("outfile")) {
                    String tmpcacheFile = map.get("outfile");
                    if (tmpcacheFile != null) cachefile = tmpcacheFile;
                }
                return new String[]{gorquery, fingerprint, projectRoot, requestId, jobid, cachefile, securityContext};
            } catch (IOException e) {
                log.error("Error when parsing redis json", e);
            }
            return new String[0];
        }).collect(Collectors.toList());

        Map<String,Future<List<String>>> futMap = runJobBatch(lstr);
        futMap.forEach((key, value) -> mont.addJob(key, value));
    }

    /**
     * For standalone gor query sparkapplication
     * @param args
     */
    public static void main(String[] args) {
        String redisUrl = args[0];
        String requestId = args[1];
        String projectDir = args[2];
        String queries = args[3];
        String fingerprints = args[4];
        String cachefiles = args[5];
        String jobids = args[6];

        SparkSession.Builder sb = new SparkSession.Builder();
        try(SparkSession sparkSession = sb
                .getOrCreate(); RedisBatchConsumer redisBatchConsumer = new RedisBatchConsumer(sparkSession, redisUrl)) {
            String[] querySplit = queries.split(";;");
            String[] fingerprintSplit = fingerprints.split(";");
            String[] cachefileSplit = cachefiles.split(";");
            String[] jobidSplit = jobids.split(";");

            List<String[]> lstr = IntStream.range(0, fingerprintSplit.length).mapToObj(i -> new String[]{querySplit[i], fingerprintSplit[i], projectDir, requestId, jobidSplit[i], cachefileSplit[i]}).collect(Collectors.toList());
            Map<String,Future<List<String>>> futMap = redisBatchConsumer.runJobBatch(lstr);

            log.info("Number of batches " + futMap.size());
            for(Future<List<String>> f : futMap.values()) {
                f.get();
            }
            log.info("Finised running all batches");
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
