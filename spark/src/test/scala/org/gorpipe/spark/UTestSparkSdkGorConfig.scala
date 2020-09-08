package org.gorpipe.spark

import java.nio.file.Paths
import java.util.stream.Collectors

import org.apache.spark.sql.SparkSession
import org.junit.{After, Assert, Before, Ignore, Test}

class UTestSparkSdkGorConfig {
  var sparkGorSession : GorSparkSession = _
  var genesPath : String = _

  @Before
  def init() {
    val project = Paths.get("../tests/data")
    genesPath = project.resolve("gor/genes.gor").toAbsolutePath.normalize().toString
    val sparkSession = SparkSession.builder().master("local[1]").getOrCreate()
    sparkGorSession = SparkGOR.createSession(sparkSession, "../ref_mini/gor_config.txt")
  }

  @Test
  @Ignore("Provide alias file")
  def testGorAliasQuery() {
    val res : java.util.stream.Stream[String] = sparkGorSession.stream("gor #genes# | top 5").map(r => r.toString)
    val res2 = res.collect(Collectors.joining("\n"))
    Assert.assertEquals("Wrong result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res2)
  }

  @Test
  def testUDFQuery() {
    val ds = sparkGorSession.dataframe("spark "+genesPath+" | top 5 | calc a udf(cos(gene_start))")
    val res2 = ds.collect().mkString("\n")
    Assert.assertEquals("Wrong result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res2)
  }

  @After
  def close() {
    sparkGorSession.close()
  }
}
