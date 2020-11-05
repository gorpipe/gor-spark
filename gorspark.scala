// SparkGOR

// To run: spark-shell --packages org.gorpipe:gor-spark:2.11.10 -I gorspark.scala
import java.nio.file.Files
import java.nio.file.Paths

import org.gorpipe.spark.SparkGOR
import spark.implicits._
import org.gorpipe.spark.GorDatasetFunctions._

sc.setLogLevel("OFF")

val sparkGorSession = SparkGOR.createSession(spark,"config/gor_config.txt","config/gor_standard_aliases.txt")
implicit val sgs = sparkGorSession

// From Example 2
val dbsnpordpath = Paths.get("dbsnp.rsOrd.parquet")
if(!Files.exists(dbsnpordpath)) {
  val ordbsnp = sgs.dataframe("select * from <(pgor ref/dbsnp/dbsnp.gorz | split rsIDs | rename rsIDs rsID) order by rsID")
  ordbsnp.write.mode("overwrite").save("dbsnp.rsOrd.parquet")
}
sgs.setCreate("#myordrssnps#","select * from dbsnp.rsOrd.parquet where rsID like 'rs222%' order by chrom, pos")
val ss = sgs.dataframe("create #myphewas# = pgor [#myordrssnps#] | varjoin -l -r phecode_gwas/Phecode_adjust_f2.gord; nor [#myphewas#] | sort -c pval_mm:n,rsID")
ss.show()

// Example 7
val myGenes = List("BRCA1","BRCA2").toDF("gene")
myGenes.createOrReplaceTempView("myGenes")
sgs.setCreateAndDefs("create #mygenes# = select gene from myGenes; def #genes# = ref/genes.gorz; def #exons# = ref/refgenes/refgenes_exons.gorz; def #dbsnp# = ref/dbsnp/dbsnp.gorz;")
sgs.setCreate("#myexons#", "gor #exons# | inset -c gene_symbol [#mygenes#]")
val exonSnps = sgs.dataframe("pgor [#myexons#] | join -segsnp -ir #dbsnp# | join -snpseg -r #genes#")
val snpCount = exonSnps.groupBy("gene_symbol").count()
snpCount.show()

// Example 8
val snpCount2 = sgs.dataframe("select count(*) from <(pgor [#myexons#] | join -segsnp -ir #dbsnp# | join -snpseg -r #genes#) group by gene_symbol")
snpCount2.show()

// Create parquet file from dbsnp.gorz
val dbsnpGorz = sgs.dataframe("select * from ref/dbsnp/dbsnp.gorz").limit(1000)
// or use spark api
// val dbsnpGorz = spark.read.format("gorsat.spark.GorDataSource").load("ref/dbsnp/dbsnp.gorz").limit(1000)
dbsnpGorz.write.save("dbsnp.parquet")
dbsnpGorz.show()

// Example how to remove create
sgs.removeCreate("#myexons#")

// Example 9
val dbsnpDf = spark.read.load("dbsnp.parquet")
val myVars = dbsnpDf.gor("calc type = if(len(reference)=len(allele),'Snp','InDel')")
myVars.createOrReplaceTempView("myVars")
sgs.setDef("#VEP#","ref/dbsnp/dbsnp.gorz")
val myVarsAnno = sgs.dataframe("select * from myVars order by chrom,pos").gor("varnorm -left reference allele | group 1 -gc reference,allele,type -set -sc rsIDs | rename set_rsIDs rsIDs | varjoin -r -l -e 'NA' <(gor #VEP# | select 1-allele,rsIDs)")
myVarsAnno.show()