package org.gorpipe.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import gorsat.DynIterator;
import gorsat.process.GenericRunnerFactory;
import gorsat.process.SparkPipeInstance;
import org.gorpipe.gor.clients.LocalFileCacheClient;
import org.gorpipe.gor.model.GorParallelQueryHandler;
import gorsat.Commands.CommandParseUtilities;
import gorsat.process.PipeInstance;
import gorsat.process.PipeOptions;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorRunner;
import org.gorpipe.gor.session.GorSessionCache;
import org.gorpipe.gor.session.ProjectContext;
import org.gorpipe.gor.session.SystemContext;
import org.gorpipe.spark.platform.*;
import redis.clients.jedis.JedisPool;

public class GeneralSparkQueryHandler implements GorParallelQueryHandler {
    GorSparkSession gpSession;

    GorClusterBase cluster;
    boolean force = false;
    public static final String queue = "GOR_CLUSTER";

    String requestID;
    String sparkRedisUri;
    private JedisPool jedisPool;

    public GeneralSparkQueryHandler(GorSparkSession gorPipeSession, String sparkRedisUri) {
        this.sparkRedisUri = sparkRedisUri;
        if (gorPipeSession != null) init(gorPipeSession);
    }

    public void setCluster(GorClusterBase cluster) {
        this.cluster = cluster;
    }

    public void init(GorSparkSession gorPipeSession) {
        this.gpSession = gorPipeSession;
        this.requestID = gorPipeSession.getRequestId();

        if (sparkRedisUri != null && sparkRedisUri.length() > 0) {
            jedisPool = SharedRedisPools.getJedisPool(JedisURIHelper.create(sparkRedisUri));

            if (cluster == null) {
                cluster = new GeneralSparkCluster(sparkRedisUri);
            }
        }
    }

    public static String[] executeSparkBatch(GorSparkSession session, String projectDir, String cacheDir, String[] fingerprints, String[] commandsToExecute, String[] jobIds, String[] cacheFiles) {
        SparkSession sparkSession = session.getSparkSession();
        String redisUri = session.getRedisUri();

        final Set<Integer> sparkJobs = new TreeSet<>();
        final Set<Integer> gorJobs = new TreeSet<>();

        Path root = Paths.get(projectDir);
        IntStream.range(0, commandsToExecute.length).forEach(i -> {
            Path cachePath = Paths.get(cacheFiles[i]);

            if(!Files.exists(root.resolve(cachePath))) {
                String command = commandsToExecute[i];
                String commandUpper = command.toUpperCase();
                String[] split = CommandParseUtilities.quoteSafeSplit(command,';');
                if (split.length > 1 || commandUpper.startsWith("SELECT ") || commandUpper.startsWith("SPARK ") || commandUpper.startsWith("GORSPARK ") || commandUpper.startsWith("NORSPARK ")) {
                    sparkJobs.add(i);
                } else {
                    gorJobs.add(i);
                }
            }
        });

        Callable<String[]> sparkRes = () -> {
            List<Object> ret = sparkJobs.parallelStream().map(i -> {
                String cmd = commandsToExecute[i];
                String[] split = CommandParseUtilities.quoteSafeSplit(cmd,';');
                String jobId = jobIds[i];
                int firstSpace = cmd.indexOf(' ');
                if(split.length==1) cmd = cmd.substring(0, firstSpace + 1) + "-j " + jobId + cmd.substring(firstSpace);
                String[] args = new String[]{cmd, "-queryhandler", "spark"};
                PipeOptions options = new PipeOptions();
                options.parseOptions(args);

                String cacheFile = cacheFiles[i];
                Path cachePath = Paths.get(cacheFile);
                if (!cachePath.isAbsolute()) cachePath = root.resolve(cacheFile);
                SparkPipeInstance pi = new SparkPipeInstance(session.getGorContext(), cachePath.toString());

                pi.subProcessArguments(options);
                pi.theInputSource().pushdownWrite(cacheFile);
                GorRunner runner = session.getSystemContext().getRunnerFactory().create();
                try {
                    runner.run(pi.getIterator(), pi.getPipeStep());
                } catch (Exception e) {
                    try {
                        if (Files.exists(cachePath))
                            Files.walk(cachePath).sorted(Comparator.reverseOrder()).forEach(path -> {
                                try {
                                    Files.delete(path);
                                } catch (IOException ioException) {
                                    // Ignore
                                }
                            });
                    } catch (IOException ioException) {
                        // Ignore
                    }
                    return e;
                }
                return cacheFile;
            }).collect(Collectors.toList());

            Optional<Exception> oe = ret.stream().filter(o -> o instanceof Exception).map(o -> (Exception)o).findFirst();
            if(oe.isPresent()) throw oe.get();

            return ret.stream().map(s -> (String)s).toArray(String[]::new);
        };

        Callable<String[]> otherRes = () -> {
            String[] newCommands = new String[gorJobs.size()];
            String[] newFingerprints = new String[gorJobs.size()];
            String[] newCacheFiles = new String[gorJobs.size()];
            String[] newJobIds = new String[gorJobs.size()];

            int k = 0;
            for (int i : gorJobs) {
                newCommands[k] = commandsToExecute[i];
                newFingerprints[k] = fingerprints[i];
                newJobIds[k] = jobIds[i];
                newCacheFiles[k] = cacheFiles[i];
                k++;
            }
            GorQueryRDD queryRDD = new GorQueryRDD(sparkSession, newCommands, newFingerprints, newCacheFiles, projectDir, cacheDir, session.getProjectContext().getGorConfigFile(), session.getProjectContext().getGorAliasFile(), newJobIds, redisUri);
            return (String[]) queryRDD.collect();
        };

        try {
            /*if (redisUri!=null && redisUri.length()>0) {
                progressCancelMonitor();
            }*/

            if (sparkJobs.size() == 0 && gorJobs.size() > 0) {
                otherRes.call();
            } else if (gorJobs.size() == 0 && sparkJobs.size() > 0) {
                sparkRes.call();
            } else if (sparkJobs.size() > 0) {
                ExecutorService executor = Executors.newFixedThreadPool(2);
                List<Callable<String[]>> callables = Arrays.asList(sparkRes, otherRes);
                executor.invokeAll(callables).forEach(future -> {
                    try {
                        future.get();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return IntStream.range(0, fingerprints.length).mapToObj(i -> jobIds[i] + "\t" + fingerprints[i] + "\t" + cacheFiles[i]).toArray(String[]::new);
    }

    @Override
    public String[] executeBatch(String[] fingerprints, String[] commandsToExecute, String[] batchGroupNames, GorMonitor mon) {
        String projectDir = gpSession.getProjectContext().getRoot();
        String cacheDir = gpSession.getProjectContext().getCacheDir();

        List<String> cacheFileList = new ArrayList<>();
        IntStream.range(0, commandsToExecute.length).forEach(i -> {
            String cachePath = cacheDir + "/" + fingerprints[i] + CommandParseUtilities.getExtensionForQuery(commandsToExecute[i], false);
            cacheFileList.add(cachePath);
        });
        String[] jobIds = Arrays.copyOf(fingerprints, fingerprints.length);

        if (jedisPool != null) {
            GorLogSubscription subscription = new RedisLogSubscription(cluster, new GorMonitorGorLogForwarder(mon), jobIds);
            subscription.start();
        }

        String[] res = executeSparkBatch(gpSession, projectDir, cacheDir, fingerprints, commandsToExecute, jobIds, cacheFileList.toArray(new String[0]));
        return Arrays.stream(res).map(s -> s.split("\t")[2]).toArray(String[]::new);
    }

    @Override
    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public void setQueryTime(Long time) {
        throw new UnsupportedOperationException("setQueryTime not supported");
    }

    @Override
    public long getWaitTime() {
        return 0;
    }
}

