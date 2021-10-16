package org.gorpipe.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.junit.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class UTestSparkGorzWrite {
    SparkSession sparkSession;

    @Before
    public void init() {
        sparkSession = SparkSession.builder()
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
                .master("local[1]").getOrCreate();
        //sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
    }

    @Test
    public void testSparkGorRead() {
        Dataset<Row> ds = sparkSession.read().format("csv").option("header","true").option("delimiter","\t").option("inferSchema","true").load("../tests/data/gor/genes.gor").limit(10);
        Assert.assertEquals("Wrong number of lines loaded from gor file in spark", 10, ds.count());
    }

    @Test
    public void testSparkGorzRead() {
        Dataset<Row> ds = sparkSession.read().format("gorsat.spark.GorDataSource").load("../tests/data/gor/genes.gorz").limit(4);
        String result = ds.collectAsList().stream().map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,DDX11L1]\n[chr1,14362,29806,WASH7P]\n[chr1,34553,36081,FAM138A]\n[chr1,53048,54936,AL627309.1]", result);
    }

    @Test
    @Ignore("Provide base64 encoder gorz file")
    public void testSparkGorGorzRead() {
        Dataset<Row> ds = sparkSession.read().format("csv").option("header","true").option("delimiter","\t").option("inferSchema","true").load("../tests/data/gor/genes64.gorz").limit(4);
        GorzFlatMap gorzFlatMap = new GorzFlatMap();
        Dataset<String> dss = ds.flatMap(gorzFlatMap, Encoders.STRING());
        String result = dss.collectAsList().stream().collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong number of lines loaded from gor file in spark", "", result);
    }

    @Test
    @Ignore("Need base64 gorz file")
    public void testSparkGorzWrite() {
        Dataset<Row> ds = sparkSession.read().format("gor").load("../tests/data/gor/genes.gorz").limit(4);
        Dataset<String> dss = ds.map((MapFunction<Row, String>) Object::toString,Encoders.STRING()).map((MapFunction<String,String>)l -> l.substring(1,l.length()-1).replace(',','\t'), Encoders.STRING());
        GorzIterator gorzIterator = new GorzIterator();

        ExpressionEncoder<Row> gorzEncoder = RowEncoder.apply(SparkGOR.gorzSchema());
        ds = dss.mapPartitions(gorzIterator, Encoders.STRING()).map((MapFunction<String,Row>)l -> {
            String[] split = l.split("\t");
            Object[] obj = {split[0], Integer.parseInt(split[1]), split[2]};
            return RowFactory.create(obj);
        }, gorzEncoder);

        GorzFlatMap gorzFlatMap = new GorzFlatMap();
        dss = ds.flatMap(gorzFlatMap, Encoders.STRING());
        String result = String.join("\n", dss.collectAsList());

        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,DDX11L1]\n[chr1,14362,29806,WASH7P]\n[chr1,34553,36081,FAM138A]\n[chr1,53048,54936,AL627309.1]", result);
    }

    @Test
    public void testSparkGorzDataSourceWrite() {
        Dataset<Row> ds = sparkSession.read().format("gor").load("../tests/data/gor/genes.gorz").limit(4);
        ds.write().format("gor").mode(SaveMode.Overwrite).save("/tmp/ds.gorz");
        ds = sparkSession.read().format("gor").schema(ds.schema()).load("/tmp/ds.gorz");
        String result = ds.collectAsList().stream().map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,DDX11L1]\n[chr1,14362,29806,WASH7P]\n[chr1,34553,36081,FAM138A]\n[chr1,53048,54936,AL627309.1]", result);
    }

    @Test
    public void testSparkGorzDataSourceWriteP() {
        Dataset<Row> ds = sparkSession.read().load("/Users/sigmar/results.parquet");
        ds.write().format("gor").mode(SaveMode.Overwrite).save("/tmp/ds.gorz");
        ds = sparkSession.read().format("gor").schema(ds.schema()).load("/tmp/ds.gorz");
        String result = ds.collectAsList().stream().map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,DDX11L1]\n[chr1,14362,29806,WASH7P]\n[chr1,34553,36081,FAM138A]\n[chr1,53048,54936,AL627309.1]", result);
    }

    @Test
    public void testSparkGorzDataSourceWriteWithNull() {
        Dataset<Row> ds = sparkSession.read().format("gor").load("../tests/data/gor/genes.gorz").limit(4);
        ds = ds.withColumn("GeneSymbol", functions.when(functions.col("Gene_Symbol").$eq$eq$eq("DDX11L1"),null).otherwise(functions.col("Gene_Symbol")));
        ds = ds.withColumn("Gene_Symbol", functions.lit(null));
        ds = ds.withColumn("gene_end", functions.col("gene_end").cast(DataTypes.LongType));
        ds = ds.withColumn("namesplit", functions.split(functions.col("GeneSymbol"),""));
        var schema = ds.schema();
        ds.write().format("gor").mode(SaveMode.Overwrite).save("/tmp/ds.gorz");

        ds = sparkSession.read().format("gor").load("/tmp/ds.gorz");
        String result = ds.collectAsList().stream().map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,,,]\n" +
                "[chr1,14362,29806,,WASH7P,WrappedArray(W, A, S, H, 7, P, )]\n" +
                "[chr1,34553,36081,,FAM138A,WrappedArray(F, A, M, 1, 3, 8, A, )]\n" +
                "[chr1,53048,54936,,AL627309.1,WrappedArray(A, L, 6, 2, 7, 3, 0, 9, ., 1, )]", result);

        ds = sparkSession.read().format("gor").schema(schema).load("/tmp/ds.gorz");
        result = ds.collectAsList().stream().map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,null,,WrappedArray()]\n" +
                "[chr1,14362,29806,null,WASH7P,WrappedArray(W, A, S, H, 7, P, )]\n" +
                "[chr1,34553,36081,null,FAM138A,WrappedArray(F, A, M, 1, 3, 8, A, )]\n" +
                "[chr1,53048,54936,null,AL627309.1,WrappedArray(A, L, 6, 2, 7, 3, 0, 9, ., 1, )]", result);
    }

    @Test
    @Ignore("Tested with localstack")
    public void testSparkS3DeltaReadWrite() {
        Dataset<Row> ds = sparkSession.read().load("../tests/data/parquet/dbsnp_test.parquet");
        ds.write().format("delta").mode(SaveMode.Overwrite).save("s3a://my-bucket/dbsnp.parquet");
        ds = sparkSession.read().load("s3a://my-bucket/dbsnp.parquet");
        Assert.assertEquals(48, ds.count());
    }

    @Test
    @Ignore("Tested with localstack")
    public void testSparkS3ParquetReadWrite() {
        Dataset<Row> ds = sparkSession.read().load("../tests/data/parquet/dbsnp_test.parquet");
        ds.write().mode(SaveMode.Overwrite).save("s3a://my-bucket/dbsnp.parquet");
        ds = sparkSession.read().load("s3a://my-bucket/dbsnp.parquet");
        Assert.assertEquals(48, ds.count());
    }

    @Test
    @Ignore("Tested with localstack")
    public void testSparkS3GorzReadWrite() throws IOException {
        Dataset<Row> ds = sparkSession.read().load("../tests/data/parquet/dbsnp_test.parquet").where("Chrom = 'chr21' or Chrom = 'chr22'");
        ds.repartition(ds.col("Chrom")).write().format("gor").mode(SaveMode.Overwrite).save("s3a://my-bucket/dbsnp.gorz");

        ds = sparkSession.read().format("csv").load("s3a://my-bucket/dbsnp.gorz/*.meta").withColumn("inputFile", org.apache.spark.sql.functions.input_file_name());
        Dataset<String> dss = ds.selectExpr("inputFile","_c0 as meta").where("meta like '## RANGE%'").map((MapFunction<Row, String>) r -> {
            String first = r.getString(0);
            int l = first.lastIndexOf('/');
            if(l == -1) l = 0;
            return first.substring(0,first.length()-5)+"\t"+first.substring(l+6,l+11)+"\t"+r.getString(1).substring(10);
        }, Encoders.STRING()).coalesce(1);
        List<String> lss = dss.collectAsList();

        Path s3path = new Path("s3a://my-bucket/dbsnp.gorz/dict.gord");
        Configuration conf = new Configuration();
        FileSystem fs = s3path.getFileSystem(conf);
        FSDataOutputStream is = fs.create(s3path);
        for(String l : lss) {
            is.writeBytes(l);
            is.write('\n');
        }
        is.close();

        Dataset<Row> dsr1 = sparkSession.read().format("gor").schema(ds.schema()).load("s3a://my-bucket/dbsnp.gorz");
        Assert.assertEquals(48, dsr1.count());
    }

    @After
    public void close() {
        sparkSession.close();
    }
}
