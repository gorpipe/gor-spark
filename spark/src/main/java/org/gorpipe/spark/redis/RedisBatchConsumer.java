package org.gorpipe.spark.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import gorsat.Commands.CommandParseUtilities;
import gorsat.process.PipeOptions;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.session.GorContext;
import org.gorpipe.gor.session.ProjectContext;
import org.gorpipe.gor.session.SystemContext;
import org.gorpipe.spark.GorQueryRDD;
import org.gorpipe.spark.GorSparkSession;
import py4j.Base64;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class RedisBatchConsumer implements VoidFunction2<Dataset<Row>, Long> {
    GorSparkSession gss;
    SystemContext sysctx;
    MonitorThread mont;
    ExecutorService es;

    public RedisBatchConsumer(SparkSession sparkSession, String redisUri) {
        gss = new GorSparkSession("");
        gss.setSparkSession(sparkSession);
        gss.redisUri_$eq(redisUri);

        SystemContext.Builder systemContextBuilder = new SystemContext.Builder();
        sysctx = systemContextBuilder
                .setServer(true)
                .setStartTime(System.currentTimeMillis())
                .build();

        es = Executors.newWorkStealingPool(4);
        mont = new MonitorThread(redisUri);
        es.submit(mont);
    }

    public void close() {
        mont.stopRunning();
        es.shutdown();
    }

    public void runGorJobs(String projectDirStr, Set<Integer> gorJobs, String[] queries, String[] fingerprints, String[] jobIds, String[] cachefiles) {
        String jobIdStr = String.join(",", jobIds);

        String[] newCommands = new String[gorJobs.size()];
        String[] newFingerprints = new String[gorJobs.size()];
        String[] newCacheFiles = new String[gorJobs.size()];
        String[] newJobIds = new String[gorJobs.size()];

        int k = 0;
        for (int i : gorJobs) {
            newCommands[k] = queries[i];
            newFingerprints[k] = fingerprints[i];
            newJobIds[k] = jobIds[i].substring(jobIds[i].lastIndexOf(':')+1);
            newCacheFiles[k] = cachefiles[i];
            k++;
        }
        String configFile = gss.getProjectContext() != null ? gss.getProjectContext().getGorConfigFile() : null;
        String aliasFile = gss.getProjectContext() != null ? gss.getProjectContext().getGorAliasFile() : null;
        GorQueryRDD gorQueryRDD = new GorQueryRDD(gss.sparkSession(), newCommands, newFingerprints, newCacheFiles, projectDirStr, "result_cache", configFile, aliasFile, newJobIds, gss.redisUri());
        Future<List<String>> fut = gorQueryRDD.toJavaRDD().collectAsync();
        mont.addJob(jobIdStr, fut);
    }

    public void runSparkJob(String projectDirStr,String cmd,String jobId,String cacheFile) {
        String shortJobId = jobId.substring(jobId.lastIndexOf(':')+1);

        int firstSpace = cmd.indexOf(' ');
        cmd = cmd.substring(0, firstSpace + 1) + "-j " + shortJobId + cmd.substring(firstSpace);
        String[] args = new String[]{cmd, "-queryhandler", "spark"};
        PipeOptions options = new PipeOptions();
        options.parseOptions(args);

        ProjectContext.Builder projectContextBuilder = new ProjectContext.Builder();
        ProjectContext prjctx = projectContextBuilder
                .setRoot(projectDirStr)
                .setCacheDir("result_cache")
                .setConfigFile(null)
                .build();
        gss.init(prjctx, sysctx, null);
        GorContext context = new GorContext(gss);

        SparkGorQuery sgq = new SparkGorQuery(context, cmd, cacheFile);
        Future<List<String>> fut = es.submit(sgq);
        mont.addJob(jobId, fut);
    }

    @Override
    public void call(Dataset<Row> v1, Long v2) {
        List<Row> rr = v1.collectAsList();
        List<String[]> lstr = rr.stream().filter(r -> r.getString(2).equals("payload")).map(r -> {
            String jobid = r.getString(1);
            String value = r.getString(3);
            ObjectMapper om = new ObjectMapper();
            try {
                String mvalue = value.substring(1,value.length()-1);
                Map<String,String> map = om.readValue(mvalue, Map.class);
                String gorquerybase = map.get("query");
                String fingerprint = map.get("fingerprint");
                String projectRoot = map.get("projectRoot");
                String requestId = map.get("request-id");
                String gorquery = new String(Base64.decode(gorquerybase));
                String cachefile = "result_cache/" + fingerprint + CommandParseUtilities.getExtensionForQuery(gorquery, false);
                if(map.containsKey("cachefile")) {
                    String tmpcacheFile = map.get("cachefile");
                    if(tmpcacheFile!=null) cachefile = tmpcacheFile;
                }
                return new String[] {gorquery, fingerprint, projectRoot, requestId, jobid, cachefile};
            } catch (IOException e) {
                e.printStackTrace();
            }
            return new String[0];
        }).collect(Collectors.toList());

        Optional<String> projectDir = lstr.stream().map(l -> l[2]).findFirst();
        if(projectDir.isPresent()) {
            String projectDirStr = projectDir.get();
            String[] queries = lstr.stream().map(l -> l[0]).toArray(String[]::new);
            String[] fingerprints = lstr.stream().map(l -> l[1]).toArray(String[]::new);
            String[] cachefiles = lstr.stream().map(l -> l[5]).toArray(String[]::new);
            String[] jobIds = lstr.stream().map(l -> l[4]).toArray(String[]::new);

            mont.setValue(jobIds, "status", "RUNNING");

            final Set<Integer> gorJobs = new TreeSet<>();
            for(int i = 0; i < queries.length; i++) {
                String cmd = queries[i];
                String commandUpper = cmd.toUpperCase();
                if (commandUpper.startsWith("SELECT ") || commandUpper.startsWith("SPARK ") || commandUpper.startsWith("GORSPARK ") || commandUpper.startsWith("NORSPARK ")) {
                    runSparkJob(projectDirStr,cmd,jobIds[i],cachefiles[i]);
                } else {
                    gorJobs.add(i);
                }
            }

            if(gorJobs.size()>0) runGorJobs(projectDirStr, gorJobs, queries, fingerprints, jobIds, cachefiles);
        }
    }
}
