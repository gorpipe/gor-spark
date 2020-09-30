package org.gorpipe.spark.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import gorsat.Commands.CommandParseUtilities;
import gorsat.Commands.Processor;
import gorsat.process.*;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import org.gorpipe.exceptions.ExceptionUtilities;
import org.gorpipe.gor.session.GorContext;
import org.gorpipe.gor.session.ProjectContext;
import org.gorpipe.gor.session.SystemContext;
import org.gorpipe.model.gor.iterators.RowSource;
import org.gorpipe.spark.GorQueryRDD;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.GorSparkUtilities;
import org.gorpipe.spark.platform.JedisURIHelper;
import org.gorpipe.spark.platform.SharedRedisPools;
import py4j.Base64;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class GorSparkRedisRunner implements Callable<String> {
    public static GorSparkRedisRunner instance;
    private SparkSession sparkSession;
    private String redisUri;
    private JedisPool jedisPool;
    private Map<String,Future<List<String>>> futureActionSet;

    public GorSparkRedisRunner(GorSparkSession sparkSession) {
        init(sparkSession.getSparkSession());
    }

    public GorSparkRedisRunner() {
        init(GorSparkUtilities.getSparkSession());
    }

    public void init(SparkSession sparkSession) {
        instance = this;
        this.sparkSession = sparkSession;
        redisUri = GorSparkUtilities.getSparkGorRedisUri();

        futureActionSet = new ConcurrentHashMap<>();
        try {
            jedisPool = SharedRedisPools.getJedisPool(JedisURIHelper.create(redisUri));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class MonitorThread implements Runnable {
        boolean running = true;

        public void stopRunning() {
            running = false;
        }

        public void run() {
            try {
                String reskey = null;

                while(running) {
                    for (String key : futureActionSet.keySet()) {
                        Future<List<String>> fut = futureActionSet.get(key);
                        String[] jobIds = key.split(",");
                        try {
                            List<String> res = fut.get(500, TimeUnit.MILLISECONDS);
                            reskey = key;
                            String[] cacheFiles = res.stream().map(s -> s.split("\t")).map(s -> s[2]).toArray(String[]::new);
                            setValues(jobIds, "result", cacheFiles);
                            setValue(jobIds, "status", "DONE");
                            break;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            reskey = key;
                            setValue(jobIds, "error", ExceptionUtilities.gorExceptionToJson(e.getCause()));
                            setValue(jobIds, "status", "FAILED");
                            break;
                        } catch (TimeoutException e) {
                            // do nothing
                        }
                    }

                    if(reskey!=null) {
                        futureActionSet.remove(reskey);
                        reskey = null;
                    }

                    if(futureActionSet.isEmpty()) Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    class SparkGorQuery implements Callable<List<String>> {
        GenericGorRunner genericGorRunner;
        RowSource iterator;
        Processor processor;
        String cachefile;

        public SparkGorQuery(RowSource iterator, Processor processor, String cachefile) {
            genericGorRunner = new GenericGorRunner();
            this.iterator = iterator;
            this.processor = processor;
            this.cachefile = cachefile;
        }

        @Override
        public List<String> call() throws Exception {
            genericGorRunner.run(iterator,processor);
            return Collections.singletonList("a\tb\t"+cachefile);
        }
    }

    @Override
    public String call() throws Exception {
        ExecutorService es = Executors.newWorkStealingPool(4);
        MonitorThread mont = new MonitorThread();
        es.submit(mont);

        GorSparkSession gss = new GorSparkSession("");
        gss.setSparkSession(sparkSession);
        gss.redisUri_$eq(redisUri);

        SystemContext.Builder systemContextBuilder = new SystemContext.Builder();
        SystemContext sysctx = systemContextBuilder
                .setServer(true)
                .setStartTime(System.currentTimeMillis())
                .build();

        StructField[] fields = {StructField.apply("_id", DataTypes.StringType, true, Metadata.empty()),StructField.apply("job", DataTypes.StringType, true, Metadata.empty()),StructField.apply("field", DataTypes.StringType, true, Metadata.empty()),StructField.apply("value", DataTypes.StringType, true, Metadata.empty())};
        StructType schema = new StructType(fields);
        StreamingQuery query = sparkSession.readStream().format("redis")
                .option("stream.read.batch.size",1)
                .option("stream.read.block", 1000)
                .option("stream.keys", "resque")
                .schema(schema)
                .load().writeStream().outputMode("update")
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (v1, v2) -> {
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
                            String cachefile;
                            if(map.containsKey("cachefile")) {
                                cachefile = map.get("cachefile");
                            } else {
                                cachefile = "result_cache/" + fingerprint + CommandParseUtilities.getExtensionForQuery(gorquerybase, false);
                            }

                            String gorquery = new String(Base64.decode(gorquerybase));
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
                        String[] cachefiles = lstr.stream().map(l -> {
                            String gorquery = l[0];
                            String fingerprint = l[1];
                            return "result_cache/" + fingerprint + CommandParseUtilities.getExtensionForQuery(gorquery, false);
                        }).toArray(String[]::new);
                        String[] jobIds = lstr.stream().map(l -> l[4]).toArray(String[]::new);
                        String jobIdStr = lstr.stream().map(l -> l[4]).collect(Collectors.joining(","));

                        setValue(jobIds, "status", "RUNNING");

                        final Set<Integer> gorJobs = new TreeSet<>();
                        for(int i = 0; i < queries.length; i++) {
                            String cmd = queries[i];
                            String commandUpper = cmd.toUpperCase();
                            if (commandUpper.startsWith("SELECT ") || commandUpper.startsWith("SPARK ") || commandUpper.startsWith("GORSPARK ") || commandUpper.startsWith("NORSPARK ")) {
                                String jobId = jobIds[i];
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
                                PipeInstance pi = new PipeInstance(context);

                                String cacheFile = cachefiles[i];
                                pi.init(cmd, false, "");
                                pi.theInputSource().pushdownWrite(cacheFile);
                                SparkGorQuery sgq = new SparkGorQuery(pi.getIterator(), pi.getPipeStep(), cacheFile);
                                Future<List<String>> fut = es.submit(sgq);
                                futureActionSet.put(jobId, fut);
                            } else {
                                gorJobs.add(i);
                            }
                        }

                        if(gorJobs.size()>0) {
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
                            GorQueryRDD gorQueryRDD = new GorQueryRDD(sparkSession, newCommands, newFingerprints, newCacheFiles, projectDirStr, "result_cache", newJobIds, redisUri);
                            Future<List<String>> fut = gorQueryRDD.toJavaRDD().collectAsync();
                            futureActionSet.put(jobIdStr, fut);
                        }
                    }
                })
                .start();
        query.awaitTermination();
        mont.stopRunning();
        es.shutdown();

        return "";
    }

    public void setValue(String[] jobIds, String field, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            for(String jobId : jobIds) {
                jedis.hset(jobId, field, value);
                jedis.expire(jobId, (int) getJobExpiration().getSeconds());
            }
        }
    }

    public void setValues(String[] jobIds, String field, String[] values) {
        try (Jedis jedis = jedisPool.getResource()) {
            for(int i = 0; i < jobIds.length; i++) {
                String jobId = jobIds[i];
                String value = values[i];
                jedis.hset(jobId, field, value);
                jedis.expire(jobId, (int) getJobExpiration().getSeconds());
            }
        }
    }

    public Duration getJobExpiration() {
        return Duration.ofMinutes(20);
    }

    public static void main(String[] args) {
        GorSparkRedisRunner grr = new GorSparkRedisRunner();
        try {
            grr.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
