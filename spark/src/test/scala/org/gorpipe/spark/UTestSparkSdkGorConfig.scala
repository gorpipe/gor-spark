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
    sparkGorSession = SparkGOR.createSession(sparkSession, project.toAbsolutePath.normalize().toString, System.getProperty("java.io.tmpdir"), null, null)
  }

  @Test
  @Ignore("Provide alias file")
  def testGorAliasQuery() {
    val res : java.util.stream.Stream[String] = sparkGorSession.stream("gor #genes# | top 5").map(r => r.toString)
    val res2 = res.collect(Collectors.joining("\n"))
    Assert.assertEquals("Wrong result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res2)
  }

  val expected = "[chr1,11868,14412,DDX11L1,0.5921715694096875]\n[chr1,14362,29806,WASH7P,0.2076618690755335]\n[chr1,34553,36081,FAM138A,-0.19199983830730022]\n[chr1,53048,54936,AL627309.1,0.5949856621187734]\n[chr1,62947,63887,OR4G11P,-0.4607112081743952]";
  @Test
  def testUDFQuery() {
    val ds = sparkGorSession.dataframe("spark "+genesPath+" | top 5 | calc a udf(cos(gene_start))")
    val res2 = ds.collect().mkString("\n")
    Assert.assertEquals("Wrong result from session query", expected, res2)
  }

  @Test
  def testSelectExpression() {
    val ds = sparkGorSession.dataframe("spark "+genesPath+" | top 5 | selectExpr *,cos(gene_start)")
    val res2 = ds.collect().mkString("\n")
    Assert.assertEquals("Wrong result from session query", expected, res2)
  }

  @Test
  def testDataSourceSQLQuery() {
    val ds = sparkGorSession.dataframe("select * from <(gor "+genesPath+" | top 5) | selectExpr *,cos(gene_start)")
    val res2 = ds.collect().mkString("\n")
    Assert.assertEquals("Wrong result from session query", expected, res2)
  }

  @Test
  def testDataSourceSQLPartitionQuery() {
    val ds = sparkGorSession.dataframe("select * from <(pgor "+genesPath+" | top 1) | selectExpr *,cos(gene_start)")
    val res2 = ds.collect();
    Assert.assertEquals("Wrong number of lines from session query", 25, res2.length)
  }

  @Test
  def testDataSourceQuery() {
    val ds = sparkGorSession.dataframe("spark {gor "+genesPath+" | top 5} | selectExpr *,cos(gene_start)")
    val res2 = ds.collect().mkString("\n")
    Assert.assertEquals("Wrong result from session query", expected, res2)
  }

  @Test
  def testDataSourcePartitionQuery() {
    val ds = sparkGorSession.dataframe("spark {pgor "+genesPath+" | top 1} | selectExpr *,cos(gene_start)")
    val res2 = ds.collect()
    Assert.assertEquals("Wrong number of lines from session query", 25, res2.length)
  }

  @After
  def close() {
    sparkGorSession.close()
  }
}
