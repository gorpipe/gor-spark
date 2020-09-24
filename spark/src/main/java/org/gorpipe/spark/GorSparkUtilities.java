package org.gorpipe.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.python.Py4JServer;
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
    private static Py4JServer py4jServer;

    private GorSparkUtilities() {}
    public static Py4JServer getPyServer() {
        return py4jServer;
    }

    public static int getPyServerPort() {
        return py4jServer != null ? py4jServer.getListeningPort() : 0;
    }

    public static String getPyServerSecret() {
        return py4jServer != null ? py4jServer.secret() : "";
    }

    public static SparkSession newSparkSession() {
        SparkConf sparkConf = new SparkConf();
        SparkSession.Builder ssb = SparkSession.builder();
        SparkSession spark = ssb.config(sparkConf).getOrCreate();

        String pyspark = System.getenv("PYSPARK_PIN_THREAD");
        if(py4jServer!=null&&pyspark!=null&&pyspark.length()>0) {
            py4jServer = new Py4JServer(spark.sparkContext().conf());
            py4jServer.start();
        }

        spark.udf().register("chartodoublearray", new CharToDoubleArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("todoublearray", new CommaToDoubleArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        spark.udf().register("todoublematrix", new CommaToDoubleMatrix(), SQLDataTypes.MatrixType());
        spark.udf().register("tointarray", new CommaToIntArray(), DataTypes.createArrayType(DataTypes.IntegerType));

        GlowBase gb = new GlowBase();
        gb.register(spark);

        return spark;
    }

    public static SparkSession getSparkSession() {
        if (!SparkSession.getDefaultSession().isEmpty()) {
            log.debug("SparkSession from default");
            spark = SparkSession.getDefaultSession().get();
        } else {
            spark = newSparkSession();
        }
        return spark;
    }

    public static List<org.apache.spark.sql.Row> stream2SparkRowList(Stream<Row> str, StructType schema) {
        return str.map(p -> new SparkGorRow(p, schema)).collect(Collectors.toList());
    }
}
