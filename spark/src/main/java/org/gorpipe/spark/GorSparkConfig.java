package org.gorpipe.spark;

import org.aeonbits.owner.Config;
import org.gorpipe.base.config.annotations.Documentation;

public interface GorSparkConfig extends Config {

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
    @DefaultValue("8g")
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

    String SPARK_KUBERNETESE_NAMESPACE = "GOR_SPARK_KUBERNETES_NAMESPACE";
    @Documentation("")
    @Key(SPARK_KUBERNETESE_NAMESPACE)
    @DefaultValue("gorkube")
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

    String SPARK_INITIAL_EXECUTORS = "GOR_SPARK_INITIAL_EXECUTORS";
    @Documentation("")
    @Key(SPARK_INITIAL_EXECUTORS)
    @DefaultValue("0")
    String getSparkInitialExecutors();

    String SPARK_MIN_EXECUTORS = "GOR_SPARK_MIN_EXECUTORS";
    @Documentation("")
    @Key(SPARK_MIN_EXECUTORS)
    @DefaultValue("0")
    String getSparkMinExecutors();

    String SPARK_MAX_EXECUTORS = "GOR_SPARK_MAX_EXECUTORS";
    @Documentation("")
    @Key(SPARK_MAX_EXECUTORS)
    @DefaultValue("100")
    String getSparkMaxExecutors();

    String SPARK_EXECUTOR_TIMEOUT = "GOR_SPARK_EXECUTOR_TIMEOUT";
    @Documentation("")
    @Key(SPARK_EXECUTOR_TIMEOUT)
    @DefaultValue("240s")
    String getSparkExecutorTimeout();

    String SPARK_PERSISTENT_VOLUME_CLAIM = "GOR_SPARK_KUBERNETES_PERSISTENT_VOLUME_CLAIM";
    @Documentation("")
    @Key(SPARK_PERSISTENT_VOLUME_CLAIM)
    @DefaultValue("pvc-gor-nfs")
    String getSparkPersistentVolumeClaim();

    String SPARK_MOUNT_PATH = "GOR_SPARK_KUBERNETES_MOUNT_PATH";
    @Documentation("")
    @Key(SPARK_MOUNT_PATH)
    @DefaultValue("/mnt/csa")
    String getSparkMountPath();
}
