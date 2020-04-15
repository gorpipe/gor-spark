package org.gorpipe.spark

import java.util.concurrent.ConcurrentHashMap

import org.gorpipe.model.genome.files.gor.RowBase
import org.gorpipe.model.gor.RowObj.splitArray
import gorsat.InputSources.Spark
import gorsat.Script.ScriptEngineFactory
import gorsat.process._
import gorsat.{BatchedPipeStepIteratorAdaptor, StringUtilities}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.gorpipe.gor.GorSession

class GorSparkSession(requestId: String) extends GorSession(requestId) with AutoCloseable {
  var sparkSession: SparkSession = _
  val createMap = new java.util.HashMap[String,String]
  val datasetMap = new ConcurrentHashMap[String, RowDataType]
  var redisUri: String = _

  if (GorInputSources.getInfo("SPARK") == null) {
      GorInputSources.register()
      GorInputSources.addInfo(new Spark.Spark)
  }

  def getSparkSession: SparkSession = {
    if(sparkSession == null) sparkSession = GorSparkUtilities.getSparkSession(null,null)
    sparkSession
  }

  def setSparkSession(ss: SparkSession) {
    sparkSession = ss
  }

  def getRedisUri: String = {
    redisUri
  }

  def where(w: String, schema: StructType): GorSparkRowFilterFunction[org.gorpipe.model.genome.files.gor.Row] = {
    new GorSparkRowFilterFunction[org.gorpipe.model.genome.files.gor.Row](w, schema)
  }

  def where(w: String, header: Array[String], gortypes: Array[String]): GorSparkRowFilterFunction[org.gorpipe.model.genome.files.gor.Row] = {
    new GorSparkRowFilterFunction[org.gorpipe.model.genome.files.gor.Row](w, header, gortypes)
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

  def spark(qry: String, sc: StructType = null): Dataset[org.gorpipe.model.genome.files.gor.Row] = {
    val pi = new PipeInstance(this.getGorContext)

    val creates = GorJavaUtilities.createMapString(createMap)
    val fullQuery = if( creates.length > 0 ) creates+"; "+qry else qry
    val args = Array[String](fullQuery)
    val options = new PipeOptions
    options.parseOptions(args)
    pi.subProcessArguments(options)

    pi.theInputSource match {
      case source: SparkRowSource =>
        val ds = source.getDataset
        if( source.isNor ) {
          ds.map(r => new SparkRow(r).asInstanceOf[org.gorpipe.model.genome.files.gor.Row])(ds.encoder.asInstanceOf[Encoder[org.gorpipe.model.genome.files.gor.Row]])
        } else {
          ds.map(r => new GorSparkRow(r).asInstanceOf[org.gorpipe.model.genome.files.gor.Row])(ds.encoder.asInstanceOf[Encoder[org.gorpipe.model.genome.files.gor.Row]])
        }
      case _ =>
        val isNor = qry.toLowerCase().startsWith("nor")
        val schema = if(sc == null) infer(pi.theIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor], pi.combinedHeader, isNor, parallel = false) else sc

        pi.subProcessArguments(options)
        val bpia = pi.theIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor]
        var gors : java.util.stream.Stream[org.gorpipe.model.genome.files.gor.Row] = null
        try {
          gors = bpia.getStream()
          if (isNor) {
            gors = gors.map(r => {
              val cs = r.otherCols()
              val sa = splitArray(cs)
              new RowBase("chrN", 0, cs, sa, null)
            })
          }

          val list: java.util.List[org.gorpipe.model.genome.files.gor.Row] = GorJavaUtilities.stream2RowList(gors/*.peek(r => r.setSchema(schema))*/)
          val ds = sparkSession.createDataset(list)(SparkGOR.gorrowEncoder)
          ds
        } finally {
          if( gors != null ) gors.close()
        }
    }
  }

  def fingerprint(cmdName: String): String = {
    val cmd = createMap.get(cmdName)
    val scriptExecutionEngine = ScriptEngineFactory.create(this.getGorContext)
    val signature = scriptExecutionEngine.getFileSignatureAndUpdateSignatureMap(cmd, scriptExecutionEngine.getUsedFiles(cmd))
    StringUtilities.createMD5(cmd+signature)
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

  def encoder(qry: String,nor: Boolean=false,parallel: Boolean=false): Encoder[org.gorpipe.model.genome.files.gor.Row] = {
    val sc = schema(qry,nor,parallel)
    RowEncoder(sc).asInstanceOf[Encoder[org.gorpipe.model.genome.files.gor.Row]]
  }

  def schema(qry: String,nor: Boolean=false,parallel: Boolean=false): StructType = {
    //gorContext.useSparkQueryHandler(true)
    val pi = new PipeInstance(this.getGorContext)
    val creates = GorJavaUtilities.createMapString(createMap)
    var fullQuery = if( creates.length > 0 ) creates+";"+qry else qry

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
    val header = pi.combinedHeader
    val schema = pi.theIterator match {
      case srs: SparkRowSource =>
        val typ = srs.getDataset.map(r => new SparkRow(r).asInstanceOf[org.gorpipe.model.genome.files.gor.Row])(SparkGOR.gorrowEncoder).limit(100).reduce(gr)
        SparkRowSource.schemaFromRow(header.split("\t"), typ)
      case _ =>
        val bpia = pi.theIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor]
        infer(bpia, pi.combinedHeader, isNor, parallel)
    }
    schema
  }

  def query(qry: String): java.util.stream.Stream[org.gorpipe.model.genome.files.gor.Row] = query(qry, null,nor = false, parallel = false)

  def query(qry: String, nor: Boolean): java.util.stream.Stream[org.gorpipe.model.genome.files.gor.Row] = query(qry, null, nor, parallel = false)

  def query(qry: String, schema: StructType): java.util.stream.Stream[org.gorpipe.model.genome.files.gor.Row] = query(qry, schema, nor = false, parallel = false)

  def query(qry: String, schema: StructType, nor: Boolean): java.util.stream.Stream[org.gorpipe.model.genome.files.gor.Row] = query(qry, schema, nor, parallel = false)

  def query(qry: String, sc: StructType, nor: Boolean, parallel: Boolean): java.util.stream.Stream[org.gorpipe.model.genome.files.gor.Row] = {
    val pi = new PipeInstance(this.getGorContext)
    val creates = GorJavaUtilities.createMapString(createMap)
    var fullQuery = if( creates.length > 0 ) creates+";"+qry else qry

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
    val bpia = pi.theIterator.asInstanceOf[BatchedPipeStepIteratorAdaptor]
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

  def gor(qry: String,schema: StructType = null): Iterator[org.gorpipe.model.genome.files.gor.Row] = {
    val it = query(qry,schema,nor = false).iterator()
    scala.collection.JavaConverters.asScalaIterator(it)
  }

  def nor(qry: String,schema: StructType = null): Iterator[org.gorpipe.model.genome.files.gor.Row] = {
    val it = query(qry,schema, nor = true).iterator()
    scala.collection.JavaConverters.asScalaIterator(it)
  }

  override def close(): Unit = {
    val gorMonitor = getSystemContext.getMonitor
    if(gorMonitor != null) gorMonitor.close()
  }
}