# SparkGOR

# To run: pyspark --packages org.gorpipe:gor-spark:2.11.10 -I gorspark.py
import types
from pyspark.mllib.common import _py2java, _java2py
from pyspark.sql import DataFrame

# helper functions
sc = spark.sparkContext

def pydataframe(self,qry):
    return _java2py(sc,self.dataframe(qry,None))

def gor(self,qry):
    df = _py2java(sc,self)
    ReflectionUtil = spark._jvm.py4j.reflection.ReflectionUtil
    Rowclass = ReflectionUtil.classForName("org.apache.spark.sql.Row")
    ct = spark._jvm.scala.reflect.ClassTag.apply(Rowclass)
    gds = spark._jvm.org.gorpipe.spark.GorDatasetFunctions(df,ct,ct)
    return _java2py(sc,gds.gor(qry,True,sgs))

def createGorSession(self):
    sgs = self._jvm.org.gorpipe.spark.SparkGOR.createSession(self._jsparkSession)
    sgs.pydataframe = types.MethodType(pydataframe,sgs)
    return sgs

def createGorSessionWithConfig(self,config,alias):
    sgs = self._jvm.org.gorpipe.spark.SparkGOR.createSession(self._jsparkSession,gorproject,cachedir,config,alias)
    sgs.pydataframe = types.MethodType(pydataframe,sgs)
    return sgs

def createGorSessionWithOptions(self,gorproject,cachedir,config,alias):
    sgs = self._jvm.org.gorpipe.spark.SparkGOR.createSession(self._jsparkSession,config,alias)
    sgs.pydataframe = types.MethodType(pydataframe,sgs)
    return sgs

setattr(DataFrame, 'gor', gor)
setattr(SparkSession, 'createGorSession', createGorSession)
setattr(SparkSession, 'createGorSessionWOptions', createGorSessionWOptions)

# init gor spark session
import os
sgs = spark.createGorSession("config/gor_config.txt","config/gor_standard_aliases.txt")

# From Examples 2,3,4
if os.path.isfile('dbsnp.rsOrd.parquet'):
  ordbsnp = sgs.pydataframe("select * from <(pgor ref/dbsnp/dbsnp.gorz | top 100000 | split rsIDs | rename rsIDs rsID) order by rsID")
  ordbsnp.write.mode("overwrite").save("dbsnp.rsOrd.parquet")
sgs.setCreate("#myordrssnps#","select * from dbsnp.rsOrd.parquet where rsID like 'rs222%' order by chrom, pos")

ss = sgs.pydataframe("create #myphewas# = pgor [#myordrssnps#] | varjoin -l -r phecode_gwas/Phecode_adjust_f2.gord; nor [#myphewas#] | sort -c pval_mm:n,rsID")
ss.toPandas()

# Example 7
import pandas as pd
myPandasGenes = pd.DataFrame(["BRCA1","BRCA2"],columns=["gene"])
myGenes = spark.createDataFrame(myPandasGenes)
myGenes.createOrReplaceTempView("myGenes")
sgs.setCreateAndDefs("create #mygenes# = select gene from myGenes; def #genes# = ref/genes.gorz; def #exons# = ref/refgenes/refgenes_exons.gorz; def #dbsnp# = ref/dbsnp/dbsnp.gorz;")
sgs.setCreate("#myexons#", "gor #exons# | inset -c gene_symbol [#mygenes#]")
exonSnps = sgs.pydataframe("pgor [#myexons#] | join -segsnp -ir #dbsnp# | join -snpseg -r #genes#")
snpCount = exonSnps.groupBy("gene_symbol").count()
snpCount.toPandas()

# Example 8
snpCount2 = sgs.pydataframe("select count(*) from <(pgor [#myexons#] | join -segsnp -ir #dbsnp# | join -snpseg -r #genes#) group by gene_symbol")
snpCount2.toPandas()

# Create parquet file from dbsnp.gorz
dbsnpGorz = sgs.pydataframe("select * from ref/dbsnp/dbsnp.gorz").limit(1000)
# or read using spark api
# dbsnpGorz = spark.read.format("gorsat.spark.GorDataSource").load("ref/dbsnp/dbsnp.gorz").limit(1001)
dbsnpGorz.write.mode("overwrite").save("dbsnp.parquet")
dbsnpGorz.toPandas()

# Example how to remove create
sgs.removeCreate("#myexons#")

# Example 9: Gor tail expression
dbsnpDf = spark.read.load("dbsnp.parquet")

myVars = dbsnpDf.gor("calc type = if(len(reference)=len(allele),'Snp','InDel')")
myVars.createOrReplaceTempView("myVars")
sgs.setDef("#VEP#","phecode_gwas/metadata/vep_single.gorz")
myVarsAnno = sgs.pydataframe("select * from myVars order by chrom,pos")
pyVarsAnno = myVarsAnno.gor("varnorm -left reference allele | group 1 -gc reference,allele,type -set -sc rsIDs | rename set_rsIDs rsIDs | varjoin -r -l -e 'NA' <(gor #VEP# | select 1-call,max_consequence)")
pyVarsAnno.toPandas()