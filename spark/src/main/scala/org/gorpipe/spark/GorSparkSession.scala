package org.gorpipe.spark

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.gorpipe.gor.model.RowBase
import org.gorpipe.model.gor.RowObj.splitArray
import gorsat.InputSources.Spark
import gorsat.Script.ScriptEngineFactory
import gorsat.Utilities.{AnalysisUtilities, StringUtilities}
import gorsat.process._
import gorsat.BatchedPipeStepIteratorAdaptor
import gorsat.Commands.CommandParseUtilities
import gorsat.Utilities.MacroUtilities.replaceAllAliases
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.gorpipe.gor.session.{EventLogger, GorSession, GorSessionCache, ProjectContext, SystemContext}

import scala.collection.JavaConverters

class GorSparkSession(requestId: String, workers: Int = 0) extends GorSession(requestId) with AutoCloseable {
  var sparkSession: SparkSession = _
  val createMap = new java.util.HashMap[String,String]
  val defMap = new java.util.HashMap[String,String]
  var creates = ""
  val datasetMap = new ConcurrentHashMap[String, RowDataType]
  var redisUri: String = _
  var fileAliasMap: java.util.Map[String,String] = _

  if (GorInputSources.getInfo("SPARK") == null) {
    GorInputSources.register()
    GorInputSources.addInfo(new Spark.Spark)
    GorInputSources.addInfo(new Spark.Select)

    GorPipeCommands.register()
    GorPipeCommands.addInfo(new gorsat.Commands.WriteSpark)
  }

  def getSparkSession: SparkSession = {
    if(sparkSession == null) sparkSession = GorSparkUtilities.getSparkSession(workers)
    sparkSession
  }

  def setSparkSession(ss: SparkSession) {
    sparkSession = ss
  }

  override
  def init(projectContext: ProjectContext, systemContext: SystemContext, cache: GorSessionCache, eventLogger: EventLogger): Unit = {
    super.init(projectContext, systemContext, cache, eventLogger)
    fileAliasMap = AnalysisUtilities.loadAliases(this.getProjectContext.getGorAliasFile, this, "gor_aliases.txt")
  }

  def getRedisUri: String = {
    redisUri
  }

  def where(w: String, schema: StructType): GorSparkRowFilterFunction[_ >: org.gorpipe.gor.model.Row] = {
    new GorSparkRowFilterFunction[org.gorpipe.gor.model.Row](w, schema)
  }

  def where(w: String, header: Array[String], gortypes: Array[String]): GorSparkRowFilterFunction[org.gorpipe.gor.model.Row] = {
    new GorSparkRowFilterFunction[org.gorpipe.gor.model.Row](w, header, gortypes)
  }

  def calc(name: String, query: String, schema: StructType): GorSparkRowMapFunction = {
    new GorSparkRowMapFunction(name, query, schema)
  }

  def calc(name: String, query: String, header: Array[String], gortypes: Array[String]): GorSparkRowMapFunction = {
    new GorSparkRowMapFunction(name, query, header, gortypes)
  }

  def analyse(q: String): GorSparkRowQueryFunction = {
    new GorSparkRowQueryFunction(q)
  }

  def analyse(ds: Dataset[Row], q: String): Dataset[Row] = {
    val ret = SparkRowSource.analyse(ds, q)
    if(ret==null) SparkRowSource.gorpipe(ds, q).asInstanceOf[Dataset[Row]]
    else ret
  }

  def query(q: String, header: Array[String]): GorSpark = {
    new GorSpark(null, false, null, q, null, null, null)
  }

  def infer(bpia: BatchedPipeStepIteratorAdaptor, hdr: String, isNor: Boolean, parallel: Boolean): StructType = {
    var schema : StructType = null
    var header = hdr
    var gors = bpia.getStream()
    try {
      if (parallel) bpia.setCurrentChrom("chr1")
      if (isNor) {
        gors = gors.map(r => {
          val cs = r.otherCols()
          val sa = splitArray(cs)
          new RowBase("chrN", 0, cs, sa, null)
        })
        var m = header.indexOf("\t")
        m = header.indexOf("\t", m + 1)
        header = header.substring(m + 1)
      }
      val gr = new GorSparkRowInferFunction()
      val typ = gors.limit(100).reduce(gr)
      schema = if( typ.isPresent ) SparkRowSource.schemaFromRow(header.split("\t"), typ.get()) else null
    } finally {
      gors.close()
    }
    schema
  }

  def replaceAliases(gorcmd: String): String = {
    val qryspl = CommandParseUtilities.quoteSafeSplit(gorcmd, ';')
    if(gorcmd.nonEmpty && fileAliasMap!=null) {
      val tmpFileAliasMap = new util.HashMap[String, String](fileAliasMap)
      AnalysisUtilities.checkAliasNameReplacement(qryspl, tmpFileAliasMap) //needs a test
      replaceAllAliases(gorcmd, tmpFileAliasMap)
    } else gorcmd
  }

  def getCreateQueries(increates: String): String = {
    val allcreates = SparkRowUtilities.createMapString(createMap, defMap, creates + increates)
    if(allcreates.nonEmpty) replaceAliases(allcreates) else allcreates
  }

  def showCreateAndDefs(): List[String] = {
    val jlist = SparkRowUtilities.createMapList(createMap, defMap, creates)
    JavaConverters.asScalaBuffer(jlist).toList
  }

  def dataframe(qry: String, sc: StructType = null): Dataset[_ <: Row] = spark(qry, sc)

  def spark(qry: String, sc: StructType = null): Dataset[_ <: Row] = {
    val qryspl = CommandParseUtilities.quoteSafeSplit(qry,';')
    val pi = new PipeInstance(this.getGorContext)
    val lastqry : String = replaceAllAliases(qryspl.last.trim, fileAliasMap)
    val slicecreates = qryspl.slice(0,qryspl.length-1)
    val increates = if(slicecreates.length > 0) slicecreates.mkString("",";",";") else ""
    val query = if(!lastqry.toLowerCase.startsWith("spark ") && !lastqry.toLowerCase.startsWith("select ")) {
      (if(sc!=null) "spark -schema {"+sc.toDDL+"} " else "spark ") + "{"+lastqry+"}"
    } else lastqry
    val createQueries = getCreateQueries(increates)
    val fullQuery = if (createQueries.nonEmpty) createQueries + query else query

    val args = Array[String](fullQuery)
    val options = new PipeOptions
    options.parseOptions(args)
    pi.subProcessArguments(options)

    pi.theInputSource match {
      case source: SparkRowSource =>
        val ds = source.getDataset
        ds
      case _ =>
        val isNor = lastqry.trim.toLowerCase().startsWith("nor")
        val schema = if (sc == null) infer(pi.getIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor], pi.getHeader(), isNor, parallel = false) else sc

        pi.subProcessArguments(options)
        val bpia = pi.getIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor]
        var gors: java.util.stream.Stream[org.gorpipe.gor.model.Row] = bpia.getStream
        try {
          if (isNor) {
            gors = bpia.getStream()
            gors = gors.map(r => {
              val cs = r.otherCols()
              val sa = splitArray(cs)
              new RowBase("chrN", 0, cs, sa, null)
            })
          }

          val list: java.util.List[Row] = GorSparkUtilities.stream2SparkRowList(gors, schema)
          val ds = sparkSession.createDataset(list)(RowEncoder.apply(schema))
          ds
        } finally {
          if (gors != null) gors.close()
        }
    }
  }

  def fingerprint(cmdName: String): String = {
    val cmd = createMap.get(cmdName)
    val scriptExecutionEngine = ScriptEngineFactory.create(this.getGorContext)
    val signature = scriptExecutionEngine.getFileSignatureAndUpdateSignatureMap(cmd, scriptExecutionEngine.getUsedFiles(cmd))
    StringUtilities.createMD5(cmd+signature)
  }

  def setDef(name: String, defstr: String) {
    defMap.put(name,defstr)
  }

  def setCreateAndDefs(cmd: String) {
    creates = cmd
  }

  def setCreate(name: String, cmd: String): String = create(name, cmd)

  def removeCreate(name: String): String = remove(name)

  def removeDef(name: String): String = {
    defMap.remove(name)
  }

  def clearDefs(): Unit = {
    defMap.clear()
  }

  def clearCreates(): Unit = {
    createMap.clear()
  }

  def clearAll(): Unit = {
    clearDefs()
    clearCreates()
    creates = ""
  }

  def create(name: String, cmd: String): String = {
    createMap.put(name, cmd)
  }

  def create(name: String, ds: Dataset[Row]): String = {
    ds.createOrReplaceTempView(name)
    createMap.put(name, "spark select * from "+name)
  }

  def remove(name: String): String = {
    createMap.remove(name)
  }

  def encoder(qry: String,nor: Boolean=false,parallel: Boolean=false): Encoder[org.gorpipe.gor.model.Row] = {
    val sc = schema(qry,nor,parallel)
    RowEncoder(sc).asInstanceOf[Encoder[org.gorpipe.gor.model.Row]]
  }

  def schema(qry: String,nor: Boolean=false,parallel: Boolean=false): StructType = {
    //gorContext.useSparkQueryHandler(true)
    val pi = new PipeInstance(this.getGorContext)
    val createQueries = SparkRowUtilities.createMapString(createMap, defMap, creates)
    var fullQuery = if( createQueries.length > 0 ) createQueries+qry else qry

    val querySplit = fullQuery.split(";")
    val lastQuery = querySplit(querySplit.length-1).trim
    var isNor = nor
    if( lastQuery.toLowerCase().startsWith("nor") ) {
      isNor = true
    } else if(nor) {
      fullQuery = (if( querySplit.length > 1 ) querySplit.slice(0,querySplit.length-1).mkString("",";","; nor ") else "nor ")+querySplit(querySplit.length-1)
    }

    val args = Array[String](fullQuery)
    val options = new PipeOptions
    options.parseOptions(args)
    pi.subProcessArguments(options)

    val gr = new GorSparkRowInferFunction()
    val header = pi.getHeader()
    val schema = pi.getIterator match {
      case srs: SparkRowSource =>
        val typ = srs.getDataset.map(r => new SparkRow(r).asInstanceOf[org.gorpipe.gor.model.Row])(SparkGOR.gorrowEncoder).limit(100).reduce(gr)
        SparkRowSource.schemaFromRow(header.split("\t"), typ)
      case _ =>
        val bpia = pi.getIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor]
        infer(bpia, pi.getHeader(), isNor, parallel)
    }
    schema
  }

  def stream(qry: String): java.util.stream.Stream[org.gorpipe.gor.model.Row] = stream(qry, null,nor = false, parallel = false)

  def stream(qry: String, nor: Boolean): java.util.stream.Stream[org.gorpipe.gor.model.Row] = stream(qry, null, nor, parallel = false)

  def stream(qry: String, schema: StructType): java.util.stream.Stream[org.gorpipe.gor.model.Row] = stream(qry, schema, nor = false, parallel = false)

  def stream(qry: String, schema: StructType, nor: Boolean): java.util.stream.Stream[org.gorpipe.gor.model.Row] = stream(qry, schema, nor, parallel = false)

  def stream(qry: String, sc: StructType, nor: Boolean, parallel: Boolean): java.util.stream.Stream[org.gorpipe.gor.model.Row] = {
    val pi = new PipeInstance(this.getGorContext)
    val createQueries = SparkRowUtilities.createMapString(createMap, defMap, creates)
    var fullQuery = if( createQueries.length > 0 ) createQueries+qry else qry

    val querySplit = fullQuery.split(";")
    val lastQuery = querySplit(querySplit.length-1).trim
    var isNor = nor
    if( lastQuery.toLowerCase().startsWith("nor") ) {
      isNor = true
    } else if(nor) {
      fullQuery = (if( querySplit.length > 1 ) querySplit.slice(0,querySplit.length-1).mkString("",";","; nor ") else "nor ")+querySplit(querySplit.length-1)
    }

    val args = Array[String](fullQuery)
    val options = new PipeOptions
    options.parseOptions(args)

    pi.subProcessArguments(options)
    val bpia = pi.getIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor]
    if (parallel) bpia.setCurrentChrom("chr1")
    var gors = bpia.getStream(parallel)
    if( isNor ) {
      gors = gors.map(r => {
        val cs = r.otherCols()
        val sa = splitArray(cs)
        new RowBase("chrN", 0, cs, sa, null)
      })
    }
    gors //.peek(r => r.setSchema(schema))
  }

  def iterator(qry: String,nor: Boolean = true, schema: StructType = null): Iterator[org.gorpipe.gor.model.Row] = {
    scala.collection.JavaConverters.asScalaIterator(stream(qry,schema,nor).iterator())
  }

  def gor(qry: String,schema: StructType = null): Iterator[org.gorpipe.gor.model.Row] = {
    val it = stream(qry,schema,nor = false).iterator()
    scala.collection.JavaConverters.asScalaIterator(it)
  }

  def nor(qry: String,schema: StructType = null): Iterator[org.gorpipe.gor.model.Row] = {
    val it = stream(qry,schema, nor = true).iterator()
    scala.collection.JavaConverters.asScalaIterator(it)
  }

  override def close(): Unit = {
    val gorMonitor = getSystemContext.getMonitor
    if(gorMonitor != null) gorMonitor.close()
  }
}