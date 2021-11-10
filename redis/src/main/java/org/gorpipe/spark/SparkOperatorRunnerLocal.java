package org.gorpipe.spark;

import org.apache.spark.sql.SparkSession;
import org.gorpipe.exceptions.GorSystemException;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.spark.redis.RedisBatchConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SparkOperatorRunnerLocal extends SparkOperatorRunner {
    private static final Logger log = LoggerFactory.getLogger(SparkOperatorRunnerLocal.class);

    public SparkOperatorRunnerLocal(GorSparkSession gorSparkSession) throws IOException {
        super(gorSparkSession);
    }

    /**
     * Keep this for debuging purposes, no kubernetes needed
     * @param sparkSession
     * @param args
     */
    @Override
    public void runJob(SparkSession sparkSession, String yaml, String prjDir, SparkOperatorSpecs sparkOperatorSpecs, GorMonitor gm, String sparkApplicationName, String[] args) {
        String redisUrl = args[0];
        String requestId = args[1];
        String projectDir = args[2];
        String queries = args[3];
        String fingerprints = args[4];
        String cachefiles = args[5];
        String jobids = args[6];
        String streamKey = args.length > 7 ? args[7] : "resque";
        try(RedisBatchConsumer redisBatchConsumer = new RedisBatchConsumer(sparkSession, redisUrl, streamKey)) {
            String[] arr = new String[]{queries, fingerprints, projectDir, requestId, jobids, cachefiles};
            List<String[]> lstr = Collections.singletonList(arr);
            Map<String, Future<List<String>>> futMap = redisBatchConsumer.runJobBatch(lstr);
            for(Future<List<String>> f : futMap.values()) {
                f.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new GorSystemException(e);
        }
    }
}
