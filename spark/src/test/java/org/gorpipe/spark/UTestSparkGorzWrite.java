package org.gorpipe.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.*;

import java.util.stream.Collectors;

public class UTestSparkGorzWrite {
    SparkSession sparkSession;

    @Before
    public void init() {
        sparkSession = SparkSession.builder().master("local[1]").getOrCreate();
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
    public void testSparkGorzWrite() {
        Dataset<Row> ds = sparkSession.read().format("gorsat.spark.GorDataSource").load("../tests/data/gor/genes.gorz").limit(4);
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
        String result = dss.collectAsList().stream().collect(Collectors.joining("\n"));

        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,DDX11L1]\n[chr1,14362,29806,WASH7P]\n[chr1,34553,36081,FAM138A]\n[chr1,53048,54936,AL627309.1]", result);
    }

    @Test
    public void testSparkGorzDataSourceWrite() {
        Dataset<Row> ds = sparkSession.read().format("gorsat.spark.GorDataSource").load("../tests/data/gor/genes.gorz").limit(4);
        ds.write().format("gorsat.spark.GorDataSource").mode(SaveMode.Overwrite).save("/tmp/ds.gorz");
        ds = sparkSession.read().format("gorsat.spark.GorDataSource").schema(ds.schema()).load("/tmp/ds.gorz");
        String result = ds.collectAsList().stream().map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong content loaded from in gorz file in spark", "[chr1,11868,14412,DDX11L1]\n[chr1,14362,29806,WASH7P]\n[chr1,34553,36081,FAM138A]\n[chr1,53048,54936,AL627309.1]", result);
    }

    @After
    public void close() {
        sparkSession.close();
    }
}
