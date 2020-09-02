package org.gorpipe.spark;

import io.projectglow.GlowBase;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.base.config.ConfigManager;
import org.gorpipe.gor.model.Row;
import org.gorpipe.spark.udfs.CharToDoubleArray;
import org.gorpipe.spark.udfs.CommaToDoubleArray;
import org.gorpipe.spark.udfs.CommaToDoubleMatrix;
import org.gorpipe.spark.udfs.CommaToIntArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GorSparkUtilities {
    private static final Logger log = LoggerFactory.getLogger(GorSparkUtilities.class);
    private static SparkSession spark;
    private static Map<String,SparkSession> sessionProfiles = new HashMap<>();

    private GorSparkUtilities() {}

    public static SparkSession  getSparkSession(String gorroot, String hostMount) {
        return getSparkSession(gorroot, hostMount, null);
    }

    public static SparkSession newSparkSession(String gorroot, String hostMount, String profile) {
        GorSparkConfig config = ConfigManager.createPrefixConfig("spark", GorSparkConfig.class);
        log.debug("SparkSession from config");
        log.info("SparkMaster from config " + config.sparkMaster());

        if (gorroot != null && hostMount == null) {
            hostMount = System.getenv("GORPROJECT_PATH");
            if (hostMount == null) {
                Path p = Paths.get("/gorproject/zeppelin-server.yaml");
                if (Files.exists(p)) {
                    //String pathline = Files.lines(p).dropWhile(k -> !k.contains("GORPROJECT_PATH")).dropWhile(k -> k.contains("GORPROJECT_PATH")).findFirst().get();
                    //hostMount = pathline.trim().split(":")[1].trim();
                }
            }
        }

        SparkConf sparkConf = new SparkConf();
        String master = config.sparkMaster();
        //activateEventLogIfSet(config, sparkConf);
        //SparkContext.getOrCreate();
        SparkSession.Builder ssb = SparkSession
                .builder()
                .appName("GorSpark " + UUID.randomUUID())
                .master(master)
                //.config("spark.ui.enabled", config.sparkUiEnabled())
                //.config("spark.jars", config.sparkJars())
                .config("spark.driver.memory", config.sparkDriverMemory())
                .config("spark.executor.memory", config.sparkExecutorMemory())
                .config("spark.executor.cores", config.sparkExecutorCores())
                .config("spark.executor.instances", config.sparkExecutorInstances())
                .config("spark.submit.deployMode", config.sparkDeployMode())
                .config("spark.kubernetes.namespace", config.getSparkKuberneteseNamespace())
                //.config("spark.ui.proxyBase","/spark")
                //.config("spark.ui.reverseProxy","true")
                //.config("spark.ui.reverseProxyUrl","https://platform.wuxinextcodedev.com/")

                //.config("spark.executor.extraClassPath","/Users/sigmar/gor-services/server/build/install/gor-scripts/lib/*")
                //.config("spark.executor.extraJavaOptions","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005")
                //.config("spark.kubernetes.allocation.batch.delay","30s")
                .config("spark.dynamicAllocation.enabled", "true")
                //.config("spark.shuffle.service.enabled","true")
                .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
                .config("spark.dynamicAllocation.minExecutors", config.getSparkMinExecutors())
                .config("spark.dynamicAllocation.maxExecutors", config.getSparkMaxExecutors())
                .config("spark.dynamicAllocation.initialExecutors", config.getSparkInitialExecutors())
                .config("spark.dynamicAllocation.executorIdleTimeout", config.getSparkExecutorTimeout());

        if (master.startsWith("k8s://")) {
            String image = profile == null ? "nextcode/spark:3.0.0" : profile;
            //String path = profile == null ? config.getSparkMountPath() : "";
            ssb = ssb
                    .config("spark.kubernetes.container.image", image)
                    .config("spark.kubernetes.executor.container.image", image)

                    //.config("spark.driver.host","noauthgorserver")
                    //.config("spark.driver.port","4099")
                    //.config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-autoscaler")

                    /*.config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.path", hostMount)
                    .config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.readOnly", "false")
                    .config("spark.kubernetes.executor.volumes.hostPath.userhome.options.path", gorroot)
                    .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.path", hostMount)
                    .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.readOnly", "false")
                    .config("spark.kubernetes.driver.volumes.hostPath.userhome.options.path", gorroot);*/

                    .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.options.claimName", config.getSparkPersistentVolumeClaim())
                    .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.mount.readOnly", profile != null)

                    .config("spark.kubernetes.container.image.pullSecrets", "dockerhub-nextcode-download-credentials")
                    .config("spark.kubernetes.container.image.pullPolicy", "Always")
                    .config("spark.kubernetes.executor.deleteOnTermination", "false")
                    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-autoscaler");
                    
            if(profile!=null) {
                ssb = ssb
                    .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.mount.path",gorroot)
                    .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.mount.subPath",gorroot);
            } else {
                ssb = ssb.config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.mount.path", config.getSparkMountPath());
            }
        } else if (master.startsWith("local")) {
            ssb = ssb.config("spark.driver.bindAddress", "127.0.0.1");
        }
            /*if( gorroot != null && hostMount != null ) {
                ssb =   ssb.config("spark.submit.deployMode","client")
                        //.config("spark.kubernetes.container.image","nextcode/spark:3.0.0-SNAPSHOT")
                        //.config("spark.kubernetes.executor.container.image","nextcode/spark:3.0.0-SNAPSHOT")

                        //.config("spark.kubernetes.executor.volumes.persistentVolumeClaim.gorproject.options.claimName", "pvc-sparkgorproject-nfs")
                        //.config("spark.kubernetes.executor.volumes.persistentVolumeClaim.gorproject.mount.path", "/gorproject")
                        //.config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.options.claimName", "pvc-sparkgor-nfs")
                        //.config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.mount.path", "/mnt/csa")

                        //.config("spark.kubernetes.container.image.pullSecrets", "dockerhub-nextcode-download-credentials")
                        //.config("spark.kubernetes.container.image.pullPolicy", "Always")

                        //.config("spark.kubernetes.driver.volumes.persistentVolumeClaim.gorproject.options.claimName", "pvc-sparkgorproject-nfs")
                        //.config("spark.kubernetes.driver.volumes.persistentVolumeClaim.gorproject.mount.path", "/gorproject")
                        //.config("spark.kubernetes.driver.volumes.persistentVolumeClaim.mntcsa.options.claimName", "pvc-sparkgor-nfs")
                        //.config("spark.kubernetes.driver.volumes.persistentVolumeClaim.mntcsa.mount.path", "/mnt/csa");


                        .config("spark.kubernetes.namespace","spark")
                        .config("spark.kubernetes.file.upload.path","/mnt/csa/tmp")

                        .config("spark.kubernetes.executor.podTemplateFile","/gorproject/template_sparkpod.yml")
                        .config("spark.kubernetes.executor.deleteOnTermination","false")

                        .config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.path",hostMount)
                        .config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.readOnly","false")
                        .config("spark.kubernetes.executor.volumes.hostPath.userhome.options.path",gorroot)
                        .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.path",hostMount)
                        .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.readOnly","false")
                        .config("spark.kubernetes.driver.volumes.hostPath.userhome.options.path",gorroot);
            }*/

        SparkSession spark = ssb.config(sparkConf).getOrCreate();

        spark.udf().register("chartodoublearray", new CharToDoubleArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("todoublearray", new CommaToDoubleArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("todoublematrix", new CommaToDoubleMatrix(), SQLDataTypes.MatrixType());
        spark.udf().register("tointarray", new CommaToIntArray(), DataTypes.createArrayType(DataTypes.IntegerType));

        GlowBase gb = new GlowBase();
        gb.register(spark);

        return spark;
    }

    public static SparkSession getSparkSession(String gorroot, String hostMount, String profile) {
        if(profile!=null) {
            if(sessionProfiles.containsKey(profile)) {
                return sessionProfiles.get(profile);
            } else {
                SparkSession spark = newSparkSession(gorroot, hostMount, profile);
                sessionProfiles.put(profile,spark);
                return spark;
            }
        } else if (spark == null) {
            if (!SparkSession.getDefaultSession().isEmpty()) {
                log.debug("SparkSession from default");
                spark = SparkSession.getDefaultSession().get();
            } else {
                spark = newSparkSession(gorroot, hostMount, profile);
            }
            //spark.udf().register("md5", s -> StringUtilities.createMD5((String) s), DataTypes.StringType);
        }
        return spark;
    }

    private static void activateEventLogIfSet(GorSparkConfig sparkGorConfig, SparkConf sparkConf) {
        if(!sparkGorConfig.eventLogDir().isEmpty()){
            String pathname = sparkGorConfig.eventLogDir();
            File eventFolder = new File(pathname);
            if(eventFolder.mkdirs()) {
                log.info("Spark event log folder created {}",  eventFolder.getAbsolutePath());
            }
            sparkConf.set("spark.eventLog.enabled", "true");
            sparkConf.set("spark.eventLog.dir",eventFolder.getAbsolutePath());
        }
    }

    public static List<org.apache.spark.sql.Row> stream2SparkRowList(Stream<Row> str, StructType schema) {
        return str.map(p -> new SparkGorRow(p, schema)).collect(Collectors.toList());
    }
}
