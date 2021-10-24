package org.gorpipe.spark;

import gorsat.Commands.CommandParseUtilities;
import gorsat.process.PipeOptions;
import gorsat.process.SparkPipeInstance;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.model.GorParallelQueryHandler;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorRunner;

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

public class GeneralSparkQueryHandler implements GorParallelQueryHandler {
    GorSparkSession gpSession;

    boolean force = false;
    public static final String queue = "GOR_CLUSTER";

    String requestID;

    public GeneralSparkQueryHandler(GorSparkSession gorPipeSession) {
        if (gorPipeSession != null) init(gorPipeSession);
    }

    public GeneralSparkQueryHandler() {}

    public void init(GorSparkSession gorPipeSession) {
        this.gpSession = gorPipeSession;
        this.requestID = gorPipeSession.getRequestId();
    }

    private static boolean isSparkQuery(String lastQuery) {
        var lastQueryLower = lastQuery.toLowerCase();
        return lastQueryLower.startsWith("select ") || lastQueryLower.startsWith("spark ") || lastQueryLower.startsWith("gorspark ") || lastQueryLower.startsWith("norspark ") || lastQuery.contains("/*+");
    }

    public static String[] executeSparkBatch(GorSparkSession session, String projectDir, String cacheDir, String[] fingerprints, String[] commandsToExecute, String[] jobIds, String[] cacheFiles) {
        SparkSession sparkSession = session.getSparkSession();

        final Set<Integer> sparkJobs = new TreeSet<>();
        final Set<Integer> gorJobs = new TreeSet<>();

        Path root = Paths.get(projectDir);
        IntStream.range(0, commandsToExecute.length).forEach(i -> {
            Path cachePath = Paths.get(cacheFiles[i]);

            if(!Files.exists(root.resolve(cachePath))) {
                String command = commandsToExecute[i];
                String[] split = CommandParseUtilities.quoteSafeSplit(command,';');
                if (split.length > 1 || isSparkQuery(command)) {
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
                String lastCmd = split[split.length-1];
                int firstSpace = lastCmd.indexOf(' ');
                lastCmd = lastCmd.substring(0, firstSpace + 1) + "-j " + jobId + lastCmd.substring(firstSpace);
                cmd = split.length==1 ? lastCmd : String.join(";", Arrays.copyOfRange(split,0,split.length-1))+";"+lastCmd;
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
            GorQueryRDD queryRDD = new GorQueryRDD(sparkSession, newCommands, newFingerprints, newCacheFiles, projectDir, cacheDir, session.getProjectContext().getGorConfigFile(), session.getProjectContext().getGorAliasFile(), newJobIds, null);
            return (String[]) queryRDD.collect();
        };

        try {
            /*if (redisUri!=null && redisUri.length()>0) {
                progressCancelMonitor();
            }*/
            var cmds = String.join(" ", commandsToExecute);
            sparkSession.sparkContext().setJobGroup("gor-spark", cmds, true);

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
    public String[] executeBatch(String[] fingerprints, String[] commandsToExecute, String[] batchGroupNames, String[] cacheFiles, GorMonitor mon) {
        String projectDir = gpSession.getProjectContext().getRoot();
        String cacheDir = gpSession.getProjectContext().getCacheDir();

        List<String> cacheFileList = new ArrayList<>();
        IntStream.range(0, commandsToExecute.length).forEach(i -> {
            String command = commandsToExecute[i];
            String[] cmdsplit = CommandParseUtilities.quoteSafeSplit(command,'|');
            String lastCmd = cmdsplit[cmdsplit.length-1].trim();
            String cachePath;
            if(lastCmd.toLowerCase().startsWith("write ")) {
                String[] lastCmdSplit = lastCmd.split(" ");
                cachePath = lastCmdSplit[lastCmdSplit.length-1];
            } else {
                cachePath = cacheDir + "/" + fingerprints[i] + CommandParseUtilities.getExtensionForQuery(command, false);
            }
            cacheFileList.add(cachePath);
        });
        String[] jobIds = Arrays.copyOf(fingerprints, fingerprints.length);
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

