package org.gorpipe.spark;

import org.aeonbits.owner.Config;
import org.gorpipe.base.config.annotations.Documentation;

public interface SparkGorConfig extends Config {

    String SPARK_MASTER = "GOR_SPARK_MASTER";
    @Documentation("")
    @Key(SPARK_MASTER)
    //@DefaultValue("k8s://https://6A0DA5D06C34D9215711B1276624FFD9.gr7.us-east-1.eks.amazonaws.com")
    //@DefaultValue("k8s://https://127.0.0.1:6443")
    //@DefaultValue("spark://nc-mbp-0116.local:7077")
    //@DefaultValue("local[*]")
    //@DefaultValue("k8s://https://192.168.64.3:8443")
    //@DefaultValue("k8s://https://127.0.0.1:6443")
    //@DefaultValue("k8s://https://6A0DA5D06C34D9215711B1276624FFD9.gr7.us-east-1.eks.amazonaws.com")
    @DefaultValue("k8s://https://kubernetes.default.svc")
    String sparkMaster();

    String SPARK_JARS = "gor.spark.jars";
    @Documentation("")
    @Key(SPARK_JARS)
    @DefaultValue("")
    String sparkJars();

    String SPARK_UI_ENABLED = "gor.spark.ui.enabled";
    @Documentation("")
    @Key(SPARK_UI_ENABLED)
    @DefaultValue("false")
    String sparkUiEnabled();

    String SPARK_EVENTLOG_DIR = "gor.spark.eventLog.dir";
    @Documentation("")
    @Key(SPARK_EVENTLOG_DIR)
    @DefaultValue("")
    String eventLogDir();

    String SPARK_DRIVER_MEM = "gor.spark.driver.memory";
    @Documentation("")
    @Key(SPARK_DRIVER_MEM)
    @DefaultValue("2000000000")
    String sparkDriverMemory();

    String SPARK_EXECUTOR_MEM= "gor.spark.executor.memory";
    @Documentation("")
    @Key(SPARK_EXECUTOR_MEM)
    @DefaultValue("8000000000")
    String sparkExecutorMemory();

    String SPARK_EXECUTOR_CORES = "gor.spark.executor.cores";
    @Documentation("")
    @Key(SPARK_EXECUTOR_CORES)
    @DefaultValue("1")
    String sparkExecutorCores();

    String SPARK_EXECUTOR_INSTANCES = "gor.spark.executor.instances";
    @Documentation("")
    @Key(SPARK_EXECUTOR_INSTANCES)
    @DefaultValue("2")
    String sparkExecutorInstances();

    String SPARK_DEPLOY_MODE = "gor.spark.deploy.mode";
    @Documentation("")
    @Key(SPARK_DEPLOY_MODE)
    @DefaultValue("client")
    String sparkDeployMode();

    String SPARK_KUBERNETESE_NAMESPACE = "gor.spark.kubernetes.namespace";
    @Documentation("")
    @Key(SPARK_KUBERNETESE_NAMESPACE)
    @DefaultValue("spark")
    String getSparkKuberneteseNamespace();

    String SPARK_REDIS_URL = "gor.spark.redis.url";
    @Documentation("The fully qualified redis url to the redis that is servicing spark jobs")
    @Key(SPARK_REDIS_URL)
    //@DefaultValue("")
        //@DefaultValue("localhost:6379/8")
    @DefaultValue("plat-redis.gorkube:6379/8")
        //@DefaultValue("docker.for.mac.localhost:6379/8")
        //@DefaultValue("host.docker.internal:6379/8")
    String sparkRedisUrl();

    //spark.kubernetes.driver.pod.name = "sfs-gor-server-legacy"
}
