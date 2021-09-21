package org.gorpipe.spark;

import gorsat.process.PipeOptions;
import gorsat.process.SparkPipeInstance;
import io.projectglow.Glow;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.spark.udfs.CharToDoubleArray;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class UTestPySpark {
    SparkSession spark;
    GorSparkSession sparkGorSession;
    SparkPipeInstance pi;

    @Before
    public void init() {
        spark = SparkSession.builder()
                .config("spark.sql.execution.arrow.pyspark.enabled","true")
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled","false")
                .config("spark.hadoop.fs.s3a.endpoint","localhost:4566")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
                .config("spark.hadoop.fs.s3a.path.style.access","true")
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.change.detection.mode","warn")
                .config("spark.hadoop.com.amazonaws.services.s3.enableV4","true")
                .config("spark.hadoop.fs.s3a.committer.name","partitioned")
                .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode","replace")
                .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
                .master("local[2]").getOrCreate();
        Glow.register(spark, false);
        spark.udf().register("chartodoublearray", new CharToDoubleArray(), DataTypes.createArrayType(DataTypes.DoubleType));
        SparkSessionFactory sparkSessionFactory = new SparkSessionFactory(spark, Paths.get(".").toAbsolutePath().normalize().toString(), System.getProperty("java.io.tmpdir"), null, null,null, null);
        GorSession session = sparkSessionFactory.create();
        sparkGorSession = (GorSparkSession) session;
        pi = new SparkPipeInstance(session.getGorContext());
    }

    @After
    public void close() {
        if (pi != null) pi.close();
    }

    private void testSparkQuery(String query, String expectedResult) {
        PipeOptions pipeOptions = new PipeOptions();
        pipeOptions.query_$eq(query);
        pi.subProcessArguments(pipeOptions);
        String result = StreamSupport.stream(Spliterators.spliteratorUnknownSize(pi.theInputSource(), 0), false).map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong results from spark query: " + query, expectedResult, result);
    }

    private void testSparkQueryWithHeader(String query, String expectedResult) {
        PipeOptions pipeOptions = new PipeOptions();
        pipeOptions.query_$eq(query);
        pi.subProcessArguments(pipeOptions);
        var header = pi.getHeader();
        if(header.startsWith("chrNOR")) {
            header = header.substring(header.indexOf('\t',header.indexOf('\t')+1)+1);
        }
        String result = header + "\n" + StreamSupport.stream(Spliterators.spliteratorUnknownSize(pi.theInputSource(), 0), false).map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong results from spark query: " + query, expectedResult, result);
    }

    @Test
    @Ignore("Need python on test machine")
    public void testPySpark() throws IOException {
        var mainpy = "import py4j\n" +
                "from pyspark.mllib.common import _py2java, _java2py\n" +
                "from pyspark.sql import *\n" +
                "from pyspark.sql.functions import *\n" +
                "from pyspark.sql.types import *\n" +
                "\n" +
                "if __name__ == \"__main__\":\n" +
                "    signature = sys.argv[1]\n" +
                "\n" +
                "    spark = SparkSession\\\n" +
                "        .builder\\\n" +
                "        .appName(signature)\\\n" +
                "        .getOrCreate()\n" +
                "\n" +
                "    sc = spark.sparkContext\n" +
                "    jds = spark._jvm.gorsat.commands.PysparkAnalysis.datasetMap.get(signature)\n" +
                "    ds = _java2py(sc,jds)\n" +
                "    pds = ds.where(\"Gene_Symbol like 'D%'\")\n" +
                "    jds = _py2java(sc,pds)\n" +
                "    spark._jvm.gorsat.commands.PysparkAnalysis.datasetMap.put(signature,jds)";
        var p = Paths.get("main.py");
        Files.writeString(p,mainpy);
        testSparkQuery("select * from ../tests/data/gor/genes.gorz limit 100 | pyspark "+p, "chr1\t11868\t14412\tDDX11L1");
    }

    @Test
    public void testGlow() throws IOException {
        var regpy = "import sys\n" +
                "from pyspark.mllib.common import _py2java, _java2py\n" +
                "from pyspark.sql import *\n" +
                "from pyspark.sql.functions import *\n" +
                "from pyspark.sql.types import *\n" +
                "\n" +
                "import pandas as pd\n" +
                "from glow import *\n" +
                "from glow.wgr.model_functions import *\n" +
                "from glow.wgr.wgr_functions import *\n" +
                "from glow import gwas\n" +
                "\n" +
                "if __name__ == \"__main__\":\n" +
                "    signature = sys.argv[1]\n" +
                "    labels = sys.argv[2]\n" +
                "\n" +
                "    spark = SparkSession\\\n" +
                "        .builder\\\n" +
                "        .appName(signature)\\\n" +
                "        .getOrCreate()\n" +
                "\n" +
                "    label_df = pd.read_csv(labels, sep='\\t', index_col=0)\n" +
                "    label_df.index = label_df.index.map(str)\n" +
                "\n" +
                "    sc = spark.sparkContext\n" +
                "    py = spark._jvm.gorsat.commands.PysparkAnalysis.datasetMap.get(signature)\n" +
                "    jds = py.getDataset()\n" +
                "    variant_dfm = _java2py(sc,jds)\n" +
                "    log_reg_df = gwas.logistic_regression(\n" +
                "                variant_dfm,\n" +
                "                label_df,\n" +
                "                correction='approx-firth',\n" +
                "                pvalue_threshold=0.05,\n" +
                "                values_column='values')\n" +
                "    log_reg_df.show()\n" +
                "    jds = _py2java(sc,log_reg_df)\n" +
                "    py.setDataset(jds)\n" +
                "    py.markDone()";
        var regpath = Paths.get("reg.py");
        Files.writeString(regpath,regpy);
        var l = "#PN\tpheno\nA\t1\nB\t1\nC\t0\nD\t0\nE\t0\nF\t0\nG\t0\nH\t0\nI\t1\nJ\t1\nK\t0\nL\t0\nM\t0\nN\t0\nO\t0\nP\t0\nQ\t0\nR\t0\nS\t0\nT\t0\nU\t0\nV\t0\nW\t0\nX\t0\n";
        var d = "Chrom\tpos\tid\tref\talt\tvalues\nchr1\t1\tid\tA\tG\t101001010110010101100010\n";
        var p = Paths.get("variants.gor");
        var pp = Paths.get("pheno.txt");
        Files.writeString(p,d);
        Files.writeString(pp,l);
        testSparkQueryWithHeader("select -schema {chrom string,pos int,id string,ref string,alt string,values string} * from "+p.toAbsolutePath()+" | replace values chartodoublearray(values) | pyspark "+regpath+" "+pp, "chr1\t11868\t14412\tDDX11L1");
    }
}
