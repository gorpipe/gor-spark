package org.gorpipe.spark

import java.nio.file.Paths
import java.util.stream.Collectors

import org.apache.spark.api.java.function.{FilterFunction, MapFunction}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.gorpipe.model.genome.files.gor.Row
import org.junit.{After, Assert, Before, Test}

class UTestGorSparkSDK {
    var sparkGorSession : GorSparkSession = _
    var genesPath : String = _

    @Before
    def init() {
        val project = Paths.get("../tests/data")
        genesPath = project.resolve("gor/genes.gor").toAbsolutePath.normalize().toString
        val sparkSession = SparkSession.builder().master("local[1]").getOrCreate();
        sparkGorSession = SparkGOR.createSession(sparkSession, project.toAbsolutePath.normalize().toString, System.getProperty("java.io.tmpdir"), 0);
    }

    @Test
    def testGorzSparkSDKQuery() {
        val res : java.util.stream.Stream[String] = sparkGorSession.query("gor "+genesPath+" | top 5").map(r => r.toString)
        val res2 = res.collect(Collectors.joining("\n"))
        Assert.assertEquals("Wring result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res2)
    }

    @Test
    def testGorzSparkSDKSpark() {
        val res = sparkGorSession.spark("spark "+genesPath+" | top 5").map(r => r.toString)(Encoders.STRING).collect().mkString("\n")
        Assert.assertEquals("Wring result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res)
    }

    @Test
    def testGorzSparkSDKSparkFilter() {
        val ds = sparkGorSession.spark("spark "+genesPath+" | top 5")
        val gorfilter : GorSparkRowFilterFunction[_ >: Row] = sparkGorSession.where("gene_start = 11868", ds.schema)
        val res = ds.map(r => new GorSparkRow(r).asInstanceOf[Row])(SparkGOR.gorrowEncoder).filter(gorfilter).map(r => r.toString)(Encoders.STRING).collect().mkString("\n")
        Assert.assertEquals("Wring result from session query", "chr1\t11868\t14412\tDDX11L1", res)
    }

    @Test
    def testGorzSparkSDKSparkCalc() {
        val ds = sparkGorSession.spark("spark "+genesPath+" | top 3")
        val gorcalc : GorSparkRowMapFunction = sparkGorSession.calc("gene_length","gene_end-gene_start", ds.schema)
        val res = ds.map(r => new GorSparkRow(r).asInstanceOf[Row])(SparkGOR.gorrowEncoder).map(gorcalc,SparkGOR.gorrowEncoder).map(r => r.toString)(Encoders.STRING).collect().mkString("\n")
        Assert.assertEquals("Wring result from session query", "chr1\t11868\t14412\tDDX11L1\t2544\nchr1\t14362\t29806\tWASH7P\t15444\nchr1\t34553\t36081\tFAM138A\t1528", res)
    }

    @Test
    def testGorzSparkSDKCreate() {
        sparkGorSession.create("res", "gor "+genesPath+" | top 5")
        val res = sparkGorSession.gor("gor [res]").map(r => r.toString).toList.mkString("\n")
        Assert.assertEquals("Wring result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res)
    }

    @After
    def close() {
        sparkGorSession.close()
    }
}
