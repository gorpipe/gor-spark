package com.nextcode.gor.spark;

import io.projectglow.GlowBase;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.gorpipe.base.config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SparkGorUtilities {
    private static final Logger log = LoggerFactory.getLogger(SparkGorUtilities.class);
    private static SparkSession spark;

    private SparkGorUtilities() {}

    public static SparkSession getSparkSession(String gorroot, String hostMount) {
        SparkGorConfig config = ConfigManager.createPrefixConfig("spark", SparkGorConfig.class);
        if( spark == null ) {
            if( !SparkSession.getDefaultSession().isEmpty() ) {
                log.debug("SparkSession from default");
                log.info("SparkMaster " + config.sparkMaster());
                spark = SparkSession.getDefaultSession().get();
            } else {
                log.debug("SparkSession from config");
                log.info("SparkMaster from config " + config.sparkMaster());

                if( gorroot != null && hostMount == null ) {
                    hostMount = System.getenv("GORPROJECT_PATH");
                    if( hostMount == null ) {
                        Path p = Paths.get("/gorproject/zeppelin-server.yaml");
                        if( Files.exists(p) ) {
                            //String pathline = Files.lines(p).dropWhile(k -> !k.contains("GORPROJECT_PATH")).dropWhile(k -> k.contains("GORPROJECT_PATH")).findFirst().get();
                            //hostMount = pathline.trim().split(":")[1].trim();
                        }
                    }
                }

                SparkConf sparkConf = new SparkConf();
                String master = config.sparkMaster();
                //activateEventLogIfSet(config, sparkConf);
                SparkSession.Builder ssb = SparkSession
                        .builder()
                        .appName("GorSpark "+ UUID.randomUUID())
                        .master(master)
                        //.config("spark.ui.enabled", config.sparkUiEnabled())
                        //.config("spark.jars", config.sparkJars())
                        .config("spark.driver.memory", config.sparkDriverMemory())
                        .config("spark.executor.memory", config.sparkExecutorMemory())
                        .config("spark.executor.cores", config.sparkExecutorCores())
                        .config("spark.executor.instances", config.sparkExecutorInstances())
                        .config("spark.submit.deployMode",config.sparkDeployMode())
                        .config("spark.kubernetes.namespace", config.getSparkKuberneteseNamespace())
                        .config("spark.ui.proxybase","/spark")
                        //.config("spark.executor.extraClassPath","/Users/sigmar/gor-services/server/build/install/gor-scripts/lib/*")
                        //.config("spark.executor.extraJavaOptions","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005")
                        //.config("spark.kubernetes.allocation.batch.delay","30s")
                        .config("spark.dynamicAllocation.enabled","true")
                        //.config("spark.shuffle.service.enabled","true")
                        .config("spark.dynamicAllocation.shuffleTracking.enabled","true")
                        .config("spark.dynamicAllocation.minExecutors","0")
                        .config("spark.dynamicAllocation.maxExecutors","100")
                        .config("spark.dynamicAllocation.initialExecutors","0")
                        .config("spark.dynamicAllocation.executorIdleTimeout","240s");

                if(master.startsWith("k8s://")) {
                    hostMount = "/gorproject";
                    gorroot = "/gorproject";

                    ssb = ssb
                        .config("spark.kubernetes.container.image","nextcode/spark:3.0.0-preview2")
                        .config("spark.kubernetes.executor.container.image","nextcode/spark:3.0.0-preview2")

                            //.config("spark.driver.host","noauthgorserver")
                            //.config("spark.driver.port","4099")
                            //.config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-autoscaler")

                        /*.config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.path", hostMount)
                        .config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.readOnly", "false")
                        .config("spark.kubernetes.executor.volumes.hostPath.userhome.options.path", gorroot)
                        .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.path", hostMount)
                        .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.readOnly", "false")
                        .config("spark.kubernetes.driver.volumes.hostPath.userhome.options.path", gorroot);*/

                        .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.gorproject.options.claimName", "pvc-sparkgorproject-nfs")
                        .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.gorproject.mount.path", "/gorproject")
                        .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.options.claimName", "pvc-sparkgor-nfs")
                        .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.mount.path", "/mnt/csa")

                        .config("spark.kubernetes.container.image.pullSecrets", "dockerhub-nextcode-download-credentials")
                        .config("spark.kubernetes.container.image.pullPolicy", "Always")
                        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-autoscaler")

                        .config("spark.kubernetes.driver.volumes.persistentVolumeClaim.gorproject.options.claimName", "pvc-sparkgorproject-nfs")
                        .config("spark.kubernetes.driver.volumes.persistentVolumeClaim.gorproject.mount.path", "/gorproject")
                        .config("spark.kubernetes.driver.volumes.persistentVolumeClaim.mntcsa.options.claimName", "pvc-sparkgor-nfs")
                        .config("spark.kubernetes.driver.volumes.persistentVolumeClaim.mntcsa.mount.path", "/mnt/csa");

                    /*if( gorroot != null ) {
                        if(hostMount==null || hostMount.length()==0) hostMount = "/gorproject";
                        ssb = ssb
                            .config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.path", hostMount)
                            .config("spark.kubernetes.executor.volumes.hostPath.userhome.mount.readOnly", "false")
                            .config("spark.kubernetes.executor.volumes.hostPath.userhome.options.path", gorroot)
                            .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.path", hostMount)
                            .config("spark.kubernetes.driver.volumes.hostPath.userhome.mount.readOnly", "false")
                            .config("spark.kubernetes.driver.volumes.hostPath.userhome.options.path", gorroot);

                        ssb = ssb.config("spark.kubernetes.driver.volumes.persistentVolumeClaim.gorproject.options.claimName", "pvc-sparkgorproject-nfs")
                            .config("spark.kubernetes.driver.volumes.persistentVolumeClaim.gorproject.mount.path", "/gorproject")
                            .config("spark.kubernetes.driver.volumes.persistentVolumeClaim.mntcsa.options.claimName", "pvc-sparkgor-nfs")
                            .config("spark.kubernetes.driver.volumes.persistentVolumeClaim.mntcsa.mount.path", "/mnt/csa");

                        ssb = ssb.config("spark.kubernetes.executor.volumes.persistentVolumeClaim.gorproject.options.claimName", "pvc-sparkgorproject-nfs")
                            .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.gorproject.mount.path", "/gorproject")
                            .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.options.claimName", "pvc-sparkgor-nfs")
                            .config("spark.kubernetes.executor.volumes.persistentVolumeClaim.mntcsa.mount.path", "/mnt/csa");
                    }*/
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

                try {
                    spark = ssb.config(sparkConf).getOrCreate();
                } catch(Exception e) {
                    e.printStackTrace();
                } finally {
                    GlowBase gb = new GlowBase();
                    gb.register(spark);
                }
            }
        }

        spark.udf().register("todoublearray", (UDF1<String, double[]>) (String s) -> {
            String[] spl = s.split(",");
            double[] ret = new double[spl.length];
            for(int i = 0; i < spl.length; i++) {
                ret[i] = Double.parseDouble(spl[i]);
            }
            return ret;
        }, DataTypes.createArrayType(DataTypes.DoubleType));

        spark.udf().register("todoublematrix", (UDF1<String, DenseMatrix>) (String s) -> {
            String[] spl = s.split(";");
            String[] subspl = spl[0].split(",");
            double[] ret = new double[spl.length*subspl.length];
            for(int i = 0; i < spl.length; i++) {
                subspl = spl[i].split(",");
                for(int k = 0; k < subspl.length; k++) {
                    ret[i*spl.length+k] = Double.parseDouble(subspl[k]);
                }
            }
            DenseMatrix dm = new DenseMatrix(subspl.length, spl.length, ret, false);
            return dm;
        }, SQLDataTypes.MatrixType());

        spark.udf().register("tointarray", (UDF1<String, int[]>) (String s) -> {
            String[] spl = s.split(",");
            int[] ret = new int[spl.length];
            for(int i = 0; i < spl.length; i++) {
                ret[i] = Integer.parseInt(spl[i]);
            }
            return ret;
        }, DataTypes.createArrayType(DataTypes.IntegerType));
        return spark;
    }

    private static void activateEventLogIfSet(SparkGorConfig sparkGorConfig, SparkConf sparkConf) {
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
}
