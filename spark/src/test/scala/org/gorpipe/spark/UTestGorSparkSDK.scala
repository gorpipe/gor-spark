package org.gorpipe.spark

import java.nio.file.Paths
import java.util.stream.Collectors

import org.apache.spark.sql.{Encoders, SparkSession}
import org.gorpipe.gor.model.Row
import org.junit.{After, Assert, Before, Test}

class UTestGorSparkSDK {
    var sparkGorSession : GorSparkSession = _
    var genesPath : String = _

    @Before
    def init() {
        val project = Paths.get("../tests/data")
        genesPath = project.resolve("gor/genes.gor").toAbsolutePath.normalize().toString
        val sparkSession = SparkSession.builder().master("local[1]").getOrCreate()
        sparkGorSession = SparkGOR.createSession(sparkSession, project.toAbsolutePath.normalize().toString, System.getProperty("java.io.tmpdir"), 0)
    }

    @Test
    def testGorzSparkSDKQuery() {
        val res : java.util.stream.Stream[String] = sparkGorSession.stream("gor "+genesPath+" | top 5").map(r => r.toString)
        val res2 = res.collect(Collectors.joining("\n"))
        Assert.assertEquals("Wrong result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res2)
    }

    @Test
    def testGorzSparkSDKSpark() {
        val res = sparkGorSession.spark("spark "+genesPath+" | top 5").map(r => r.toString)(Encoders.STRING).collect().mkString("\n")
        Assert.assertEquals("Wrong result from session query", "[chr1,11868,14412,DDX11L1]\n[chr1,14362,29806,WASH7P]\n[chr1,34553,36081,FAM138A]\n[chr1,53048,54936,AL627309.1]\n[chr1,62947,63887,OR4G11P]", res)
    }

    @Test
    def testGorzSparkSDKSparkFilter() {
        val ds = sparkGorSession.spark("spark "+genesPath+" | top 5")
        val gorfilter : GorSparkRowFilterFunction[_ >: Row] = sparkGorSession.where("gene_start = 11868", ds.schema)
        val res = ds.map(r => new GorSparkRow(r).asInstanceOf[Row])(SparkGOR.gorrowEncoder).filter(gorfilter).map(r => r.toString)(Encoders.STRING).collect().mkString("\n")
        Assert.assertEquals("Wrong result from session query", "chr1\t11868\t14412\tDDX11L1", res)
    }

    @Test
    def testGorzSparkSDKSparkCalc() {
        val ds = sparkGorSession.spark("spark "+genesPath+" | top 3")
        val gorcalc : GorSparkRowMapFunction = sparkGorSession.calc("gene_length","gene_end-gene_start", ds.schema)
        val res = ds.map(r => new GorSparkRow(r).asInstanceOf[Row])(SparkGOR.gorrowEncoder).map(gorcalc,SparkGOR. gorrowEncoder).map(r => r.toString)(Encoders.STRING).collect().mkString("\n")
        Assert.assertEquals("Wrong result from session query", "chr1\t11868\t14412\tDDX11L1\t2544\nchr1\t14362\t29806\tWASH7P\t15444\nchr1\t34553\t36081\tFAM138A\t1528", res)
    }

    @Test
    def testGorzSparkSDKCreate() {
        sparkGorSession.create("res", "gor "+genesPath+" | top 5")
        val res = sparkGorSession.gor("gor [res]").map(r => r.toString).toList.mkString("\n")
        Assert.assertEquals("Wrong result from session query", "chr1\t11868\t14412\tDDX11L1\nchr1\t14362\t29806\tWASH7P\nchr1\t34553\t36081\tFAM138A\nchr1\t53048\t54936\tAL627309.1\nchr1\t62947\t63887\tOR4G11P", res)
    }

    @Test
    def testPaperQuery(): Unit = {
        val spark = sparkGorSession.sparkSession
        import spark.implicits._
        val myGenes = List("BRCA1","BRCA2").toDF("gene")
        myGenes.createOrReplaceTempView("myGenes")
        sparkGorSession.setCreateAndDefs("create #mygenes# = select gene from myGenes; def #genes# = gor/genes.gorz; def #exons# = gor/ensgenes_exons.gorz; def #dbsnp# = gor/dbsnp_test.gorz;")
        sparkGorSession.setCreate("#myexons#", "gor #exons# | inset -c gene_symbol [#mygenes#]")
        val exonSnps = sparkGorSession.dataframe("pgor [#myexons#] | join -segsnp -ir #dbsnp# | join -snpseg -r #genes#")
        val snpCount = exonSnps.groupBy("gene_symbol").count().collect().mkString("\n")
        Assert.assertEquals("Wrong result","",snpCount)

        val snpCount2 = sparkGorSession.dataframe("select count(*) from <(pgor [#myexons#] | join -segsnp -ir #dbsnp# | join -snpseg -r #genes#) group by gene_symbol").collect().mkString("\n")
        Assert.assertEquals("Wrong result","",snpCount2)
    }

    @Test
    def testTempTableQuery(): Unit = {
        val spark = sparkGorSession.sparkSession
        import spark.implicits._
        val myGenes = List("BRCA1","BRCA2").toDF("gene")
        myGenes.createOrReplaceTempView("brcaGenes")
        val exonSnps = sparkGorSession.dataframe("create #mygenes# = select gene from brcaGenes; nor [#mygenes#]")
        val snpCount = exonSnps.collect().mkString("\n")

        Assert.assertEquals("Wrong result","[BRCA1]\n[BRCA2]",snpCount)
    }

    @Test
    def testDependentCreatesQuery(): Unit = {
        val spark = sparkGorSession.sparkSession
        import spark.implicits._
        val myGenes = List("OR4G11P","OR4F5").toDF("gene")
        myGenes.createOrReplaceTempView("myGenes")
        val exonSnps = sparkGorSession.dataframe("create #mygenes# = select gene from myGenes; create mmm = nor [#mygenes#]; create xxx = gor "+genesPath+" | top 100; create yyy = gor [xxx] | top 10; gor [yyy] | inset -c gene_symbol [mmm]")
        val snpCount = exonSnps.collect().mkString("\n")

        Assert.assertEquals("Wrong result","[chr1,62947,63887,OR4G11P]\n[chr1,69090,70008,OR4F5]",snpCount)
    }

    @Test
    def testPaperQuery2(): Unit = {
        import org.gorpipe.spark.GorDatasetFunctions._
        val spark = sparkGorSession.sparkSession
        implicit val sgs: GorSparkSession = sparkGorSession
        val dbsnpDf = spark.read.parquet("../tests/data/parquet/dbsnp_test.parquet")
        val myVars = dbsnpDf.gor("calc type = if(len(reference)=len(allele),'Snp','InDel')")
        myVars.createOrReplaceTempView("myVars")
        sparkGorSession.setDef("#VEP#","gor/dbsnp_test.gorz")
        val myVarsAnno = sparkGorSession.dataframe("select * from myVars order by chrom,pos").gor("varnorm -left reference allele | group 1 -gc reference,allele,type -set -sc differentrsids | rename set_differentrsids rsIDs | varjoin -r -l -e 'NA' <(gor #VEP# | select 1-allele,rsIDs)")
        val res = myVarsAnno.limit(1).collect().mkString("\n")
        Assert.assertEquals("Wrong result","[chr1,10179,N,NC,InDel,rs367896724,NA]",res)
    }

    @After
    def close() {
        sparkGorSession.close()
    }
}
