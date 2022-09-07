package org.gorpipe.spark

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors
import org.apache.spark.sql.{Encoders, SparkSession}
import org.gorpipe.gor.model.Row
import org.gorpipe.spark.GorDatasetFunctions.addCustomFunctions
import org.junit.{After, Assert, Before, Ignore, Test}

class UTestGorSparkSDK {
    var sparkGorSession : GorSparkSession = _
    var genesPath : String = _
    var goraliaspath : Path = _
    var gorconfigpath : Path = _

    @Before
    def init() {
        val project = Paths.get("../tests/data")
        goraliaspath = project.resolve("goralias.txt")
        gorconfigpath = project.resolve("gorconfig.txt")
        genesPath = project.resolve("gor/genes.gor").toAbsolutePath.normalize().toString
        val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()
        Files writeString(goraliaspath, "#genesalias#\tgor/genes.gorz\n")
        Files writeString(gorconfigpath, "buildPath\tref_mini/chromSeq\nbuildSizeFile\tref_mini/buildsize.gor\nbuildSplitFile\tref_mini/buildsplit.txt\n")
        sparkGorSession = SparkGOR.createSession(sparkSession, project.toAbsolutePath.normalize().toString, System.getProperty("java.io.tmpdir"), gorconfigpath.toAbsolutePath.normalize().toString, goraliaspath.toAbsolutePath.normalize().toString)
    }

    @Test
    def testPushdownRange(): Unit = {
        var res = sparkGorSession.dataframe("gor -p chr2 ../../tests/data/gor/genes.gorz")
        res = res.where("gene_start >= 100000000 and gene_start <= 100100000")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from select empty cmd","[chr2,100016937,100106497,REV1]",res2)
    }

    @Test
    def testSelectCmdEmpty(): Unit = {
        val res = sparkGorSession.dataframe("select * from <(cmd {date})")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from select empty cmd","",res2)
    }

    @Test
    def testSelectCmd(): Unit = {
        val res = sparkGorSession.dataframe("select * from <(cmd -n {bash -c 'for i in {10..12}; do echo $i; done;'})")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from select cmd","[11]\n[12]",res2)
    }

    @Test
    def testSelectCmdHeaderless(): Unit = {
        val res = sparkGorSession.dataframe("select * from <(cmd -n -h {bash -c 'for i in {1..2}; do echo $i; done;'})")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from select cmd no header","[1]\n[2]",res2)
    }

    @Test
    def testSelectCmdHeaderlessString(): Unit = {
        val res = sparkGorSession.dataframe("select * from <(cmd -n -h {bash -c 'for i in {1..2}; do echo \"hey$i\"; done;'})")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from select cmd no header string","[hey1]\n[hey2]",res2)
    }

    @Test
    def testSelectNorrows(): Unit = {
        val res = sparkGorSession.dataframe("select * from <(norrows 2)")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from select norrows","[0]\n[1]",res2)
    }

    @Test
    def testSelectWithSchema(): Unit = {
        val res = sparkGorSession.dataframe("select * from <(norrows 2)",StructType.fromDDL("hey int"))
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("StructField(hey,IntegerType,true)",res.schema.mkString)
        Assert.assertEquals("Wrong results from select norrows","[0]\n[1]",res2)
    }

    @Test
    @Ignore("Timeout")
    def testSeq(): Unit = {
        val res = sparkGorSession.dataframe("gor gor/dbsnp_test.gorz | top 1 | seq")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from seq","[chr1,10179,C,CC,rs367896724,taaccctaac(c)taaccctaac]",res2)
    }

    @Test
    @Ignore("Timeout")
    def testCreateSeq(): Unit = {
        val res = sparkGorSession.dataframe("create xxx = gor gor/dbsnp_test.gorz | top 1 | seq; gor [xxx]")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from create seq","[chr1,10179,C,CC,rs367896724,taaccctaac(c)taaccctaac]",res2)
    }

    @Test
    def testNorrows(): Unit = {
        val res = sparkGorSession.dataframe("norrows 2")
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from norrows","[0]\n[1]",res2)
    }

    @Test
    def testNestedNorrows(): Unit = {
        val res = sparkGorSession.dataframe("nor <(norrows 2)").gor("calc x 'x'")(sparkGorSession)
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from nested norrows","0\tx\n1\tx",res2)
    }

    @Test
    def testGorWhereDouble(): Unit = {
        val res = sparkGorSession.dataframe("gor gor/genes.gor | calc f 1.0").gor("where gene_end > 29805.0 | where f < 2")(sparkGorSession)
        val res2 = res.limit(1).collect().mkString("\n")
        Assert.assertEquals("Wrong results from nested norrows","[chr1,14362,29806,WASH7P,1.0]",res2)
    }

    @Test
    def testGorWhere(): Unit = {
      val res = sparkGorSession.dataframe("gor gor/genes.gor").gor("where gene_end > 29805")(sparkGorSession)
      val res2 = res.limit(1).collect().mkString("\n")
      Assert.assertEquals("Wrong results from nested norrows","[chr1,14362,29806,WASH7P]",res2)
    }

    @Test
    def testGorWhere2(): Unit = {
      val res = sparkGorSession.dataframe("gor gor/genes.gor").gor("where gene_end > 29805")(sparkGorSession).gor("where gene_end < 29807")(sparkGorSession)
      val res2 = res.collect().mkString("\n")
      Assert.assertEquals("Wrong results from nested norrows","[chr1,14362,29806,WASH7P]",res2)
    }

    @Test
    def testPGor(): Unit = {
        val df = sparkGorSession.dataframe("pgor gor/genes.gor | top 1")
        val res2 = df.collect().mkString("\n")
        Assert.assertEquals("Wrong results from nested gorrows", "[chr1,4]", res2)
    }

    @Test
    def testGorrowsWithSchema(): Unit = {
        val df = sparkGorSession.dataframe("gorrows -p chr1:1-5")
        val res = df.gorschema("where pos > 3",df.schema)(sparkGorSession)
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong results from nested gorrows", "[chr1,4]", res2)
    }

    @Test
    def testAdjustCommandWithSchema(): Unit = {
        val wanted = "CHROM\tPOS\tP_VAlUES\tGC\tQQ\tBONF\tHOLM\tSS\tSD\tBH\tBY\n" +
          "chr1\t1\t0.02\t0.40413\t0.1\t0.1\t0.1\t0.096079\t0.096079\t0.1\t0.22833\n" +
          "chr1\t2\t0.04\t0.46142\t0.3\t0.2\t0.16\t0.18463\t0.15065\t0.1\t0.22833\n" +
          "chr1\t3\t0.06\t0.5\t0.5\t0.3\t0.18\t0.2661\t0.16942\t0.1\t0.22833\n" +
          "chr1\t4\t0.08\t0.53011\t0.7\t0.4\t0.18\t0.34092\t0.16942\t0.1\t0.22833\n" +
          "chr1\t5\t0.1\t0.55527\t0.9\t0.5\t0.18\t0.40951\t0.16942\t0.1\t0.22833\n"
        val cont = "CHROM\tPOS\tP_VAlUES\nchr1\t1\t0.02\n" +
          "chr1\t2\t0.04\n" +
          "chr1\t3\t0.06\n" +
          "chr1\t4\t0.08\n" +
          "chr1\t5\t0.1\n"

        val p = Paths.get("basic.gor")
        try {
            Files.writeString(p, cont)

            val schema = StructType(
                Array(StructField("CHROM", StringType, nullable = true),
                    StructField("POS", IntegerType, nullable = true),
                    StructField("P_VALUES", DoubleType, nullable = true),
                    StructField("GC", DoubleType, nullable = true),
                    StructField("QQ", DoubleType, nullable = true),
                    StructField("BONF", DoubleType, nullable = true),
                    StructField("HOLM", DoubleType, nullable = true),
                    StructField("SS", DoubleType, nullable = true),
                    StructField("SD", DoubleType, nullable = true),
                    StructField("BH", DoubleType, nullable = true),
                    StructField("BY", DoubleType, nullable = true)))
            val df = sparkGorSession.dataframe("gor "+p.toAbsolutePath.toString).gorschema("adjust -pc 3 -gcc -bonf -holm -ss -sd -bh -by -qq", schema)(sparkGorSession)
            val res2 = df.collect().mkString("\n")
            val expected: String = wanted.split("\n").drop(1).map(s => '['+s.replace("\t",",")+']').mkString("\n")
            Assert.assertEquals("Wrong results from nested gorrows", expected, res2)
        } finally {
            Files.deleteIfExists(p)
        }
    }

    @Test
    def testWriteGorrows(): Unit = {
        val path = Files.createTempFile("gor",".gorz");
        try {
            val res = sparkGorSession.dataframe("gorrows -p chr1:1-5").gor("write " + path)(sparkGorSession)
            val res2 = res.collect().mkString("\n")
            Assert.assertEquals("Wrong results from nested gorrows", "", res2)
        } finally {
            Files.deleteIfExists(path);
        }
    }


    @Test
    def testGorAlias() {
        sparkGorSession.setCreate("test","gor #genesalias# | top 1")
        val res = sparkGorSession.dataframe("gor #genesalias# [test] | top 1").gor("join -segseg #genesalias#")(sparkGorSession)
        val res2 = res.collect().mkString("\n")
        Assert.assertEquals("Wrong result from alias query","[chr1,11868,14412,DDX11L1,0,11868,14412,DDX11L1]\n[chr1,11868,14412,DDX11L1,0,14362,29806,WASH7P]",res2)
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
    @Ignore("Timeout")
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
    @Ignore("Timeout")
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
    @Ignore("Slow test")
    def testTempTableQueryCacheTest(): Unit = {
        val spark = sparkGorSession.sparkSession
        import spark.implicits._
        var myGenes = List("BRCA1","BRCA2").toDF("gene")
        myGenes.createOrReplaceTempView("brcaGenes")
        var exonSnps = sparkGorSession.dataframe("create #mygenes# = select gene from brcaGenes; nor [#mygenes#]")
        var snpCount = exonSnps.sort("gene").collect().mkString("\n")
        Assert.assertEquals("Wrong result","[BRCA1]\n[BRCA2]",snpCount)

        myGenes = List("BRCA1","BRCA2","BRCA3").toDF("gene")
        myGenes.createOrReplaceTempView("brcaGenes")
        exonSnps = sparkGorSession.dataframe("create #mygenes# = select gene from brcaGenes; nor [#mygenes#]")
        snpCount = exonSnps.sort("gene").collect().mkString("\n")

        Assert.assertEquals("Wrong result","[BRCA1]\n[BRCA2]\n[BRCA3]",snpCount)
    }

    @Test
    @Ignore("Slow test")
    def testTempTableFileQueryCacheTest(): Unit = {
        val brcaPath = Paths.get("../tests/data/brcaGenes.tsv")
        try {
            Files.writeString(brcaPath, "#gene\nBRCA1\nBRCA2\n");
            var exonSnps = sparkGorSession.dataframe("create #mygenes# = select * from brcaGenes.tsv; nor [#mygenes#]")
            var snpCount = exonSnps.sort("gene").collect().mkString("\n")
            Assert.assertEquals("Wrong result", "[BRCA1]\n[BRCA2]", snpCount)

            Files.writeString(brcaPath, "#gene\nBRCA1\nBRCA2\nBRCA3\n")
            exonSnps = sparkGorSession.dataframe("create #mygenes# = select * from brcaGenes.tsv; nor [#mygenes#]")
            snpCount = exonSnps.sort("gene").collect().mkString("\n")

            Assert.assertEquals("Wrong result", "[BRCA1]\n[BRCA2]\n[BRCA3]", snpCount)
        } finally {
            Files.deleteIfExists(brcaPath)
        }
    }

    @Test
    @Ignore("Slow test")
    def testTempTableFileNestedQueryCacheTest(): Unit = {
        val brcaPath = Paths.get("../tests/data/brcaGenes.tsv")
        try {
            Files.writeString(brcaPath, "#gene\nBRCA1\nBRCA2\n");
            var exonSnps = sparkGorSession.dataframe("create #mygenes# = select * from <(nor brcaGenes.tsv); nor [#mygenes#]")
            var snpCount = exonSnps.sort("gene").collect().mkString("\n")
            Assert.assertEquals("Wrong result", "[BRCA1]\n[BRCA2]", snpCount)

            Files.writeString(brcaPath, "#gene\nBRCA1\nBRCA2\nBRCA3\n")
            exonSnps = sparkGorSession.dataframe("create #mygenes# = select * from <(nor brcaGenes.tsv); nor [#mygenes#]")
            snpCount = exonSnps.sort("gene").collect().mkString("\n")

            Assert.assertEquals("Wrong result", "[BRCA1]\n[BRCA2]\n[BRCA3]", snpCount)
        } finally {
            Files.deleteIfExists(brcaPath)
        }
    }

    @Test
    @Ignore("Timeout")
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
    @Ignore("Timeout")
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
        Files.deleteIfExists(goraliaspath)
        Files.deleteIfExists(gorconfigpath)
        if (sparkGorSession != null) sparkGorSession.close()
    }
}
