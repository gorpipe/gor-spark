package org.gorpipe.spark

import java.nio.file.{Files, Paths}
import gorsat.Commands.{Analysis, CommandParseUtilities}
import org.gorpipe.model.gor.RowObj
import gorsat.DynIterator.DynamicRowSource
import gorsat.Outputs.OutFile
import gorsat.QueryHandlers.GeneralQueryHandler
import gorsat.Script.{ScriptEngineFactory, ScriptExecutionEngine}
import gorsat.Utilities.AnalysisUtilities
import gorsat.Utilities.MacroUtilities.replaceAllAliases
import gorsat.process._
import gorsat.spark.ReceiveQueryHandler
import gorsat.{BatchedPipeStepIteratorAdaptor, DynIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, TaskContext}
import org.gorpipe.gor.session.GorContext
import org.gorpipe.gor.function.{GorRowFilterFunction, GorRowMapFunction}
import org.gorpipe.gor.binsearch.GorIndexType
import org.gorpipe.gor.model.{GenomicIterator, GenomicIteratorBase, RowBase}
import org.gorpipe.model.gor.iterators.RowSource

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class GorFunctions[T: ClassTag](rdd: RDD[T]) {
  def gor(cmd: String, header: String) = new GorRDD(rdd, cmd, header, true)

  def nor(cmd: String, header: String) = new GorRDD(rdd, cmd, header, false)
}

object GorFunctions {
  implicit def addCustomFunctions[T: ClassTag](rdd: RDD[T]): GorFunctions[T] = new GorFunctions(rdd)
}

class GorDatasetFunctions[T: ClassTag](ds: Dataset[T])(implicit tag: ClassTag[T]) {
  def gorrow(): Dataset[org.gorpipe.gor.model.Row] = {
    ds.map(r => {
      if (r.isInstanceOf[org.gorpipe.gor.model.Row]) r
      else new GorSparkRow(r.asInstanceOf[Row])
    })(ds.encoder.asInstanceOf[Encoder[Any]]).asInstanceOf[Dataset[org.gorpipe.gor.model.Row]]
  }

  def gorpipe(cmd: String, outSchema: StructType = null): Dataset[org.gorpipe.gor.model.Row] = {
    val gpf = new GorPipeFunction(cmd, ds.schema.fieldNames.mkString("\t") /*, outSchema*/)

    val encoder: Encoder[org.gorpipe.gor.model.Row] = if (outSchema == null) SparkGOR.gorrowEncoder else RowEncoder.apply(outSchema).asInstanceOf[Encoder[org.gorpipe.gor.model.Row]]
    ds.asInstanceOf[Dataset[org.gorpipe.gor.model.Row]].mapPartitions(gpf, encoder)
  }

  def pipe(cmd: String, header: String): Dataset[String] = {
    val pf = new PipeFunction(cmd, header)
    ds.asInstanceOf[Dataset[String]].mapPartitions(pf, Encoders.STRING)
  }

  def pipe(cmd: String): Dataset[String] = {
    val pf = new PipeFunction(cmd, ds.schema.fieldNames.mkString("\t"))
    ds.asInstanceOf[Dataset[String]].mapPartitions(pf, Encoders.STRING)
  }

  def gorwhere(cmd: String): Dataset[org.gorpipe.gor.model.Row] = {
    val gw = new GorSparkRowFilterFunction[org.gorpipe.gor.model.Row](cmd, ds.schema)

    tag.runtimeClass match {
      case _ => ds.asInstanceOf[Dataset[org.gorpipe.gor.model.Row]].filter(gw)
    }
  }

  def calc(colname: String, cmd: String): Dataset[org.gorpipe.gor.model.Row] = {
    val gc = new GorSparkRowMapFunction(colname, cmd, ds.schema)
    val cn = tag.runtimeClass.getName
    val ds2 = if (cn.equals("org.apache.spark.sql.Row")) {
      ds.asInstanceOf[Dataset[Row]].map(row => new GorSparkRow(row))(ds.asInstanceOf[Dataset[GorSparkRow]].encoder).asInstanceOf[Dataset[org.gorpipe.gor.model.Row]]
    } else if (cn.equals("java.lang.String")) {
      ds.asInstanceOf[Dataset[String]].map(row => RowObj(row))(SparkGOR.gorrowEncoder)
    } else {
      ds.asInstanceOf[Dataset[org.gorpipe.gor.model.Row]]
    }
    val sc2 = gc.getSchema
    val enc = RowEncoder.apply(sc2).asInstanceOf[Encoder[org.gorpipe.gor.model.Row]]
    ds2.map(gc, enc)
  }

  def gor(gorcmd: String, inferschema: Boolean = true)(implicit sgs: GorSparkSession): Dataset[org.gorpipe.gor.model.Row] = {
    val ocmd = sgs.replaceAliases(gorcmd)
    val nor = SparkRowSource.checkNor(ds.schema.fields)
    val header = if(nor) "ChromNOR\tPosNOR\t"+ds.schema.fieldNames.mkString("\t") else ds.schema.fieldNames.mkString("\t")
    val cn = tag.runtimeClass.getName
    val dsr = if (cn.equals("org.apache.spark.sql.Row")) {
      if (nor) ds.asInstanceOf[Dataset[Row]].map(row => new SparkRow(row).asInstanceOf[org.gorpipe.gor.model.Row])(SparkGOR.gorrowEncoder)
      else ds.asInstanceOf[Dataset[Row]].map(row => new GorSparkRow(row).asInstanceOf[org.gorpipe.gor.model.Row])(SparkGOR.gorrowEncoder)
    } else if (cn.equals("java.lang.String")) {
      ds.asInstanceOf[Dataset[String]].map(row => RowObj(row))(SparkGOR.gorrowEncoder)
    } else {
      ds.asInstanceOf[Dataset[org.gorpipe.gor.model.Row]]
    }

    val allquery = sgs.getCreateQueries("") + ocmd
    val gorCommands = CommandParseUtilities.quoteSafeSplitAndTrim(allquery, ';')

    val see: ScriptExecutionEngine = ScriptEngineFactory.create(sgs.getGorContext)
    val fixedQuery = see.execute(gorCommands, false)
    val fixedCommands = CommandParseUtilities.quoteSafeSplitAndTrim(fixedQuery, ';')
    val cmd = fixedCommands.last

    if (inferschema) {
      val gs = new GorSpark(header, nor, SparkGOR.gorrowEncoder.schema, cmd, sgs.getProjectContext.getRoot)
      val gr = new GorSparkRowInferFunction()

      val gsm = new GorSparkMaterialize(header, nor, SparkGOR.gorrowEncoder.schema, cmd, sgs.getProjectContext.getRoot, 100)
      val mu = if(nor) ds.asInstanceOf[Dataset[Row]].map(row => new NorSparkRow(row).asInstanceOf[org.gorpipe.gor.model.Row])(SparkGOR.gorrowEncoder) else ds.asInstanceOf[Dataset[Row]].map(row => new GorSparkRow(row).asInstanceOf[org.gorpipe.gor.model.Row])(SparkGOR.gorrowEncoder)

      val row = mu.mapPartitions(gsm, SparkGOR.gorrowEncoder).limit(100).reduce(gr)
      val schema = SparkRowSource.schemaFromRow(gs.query().getHeader().split("\t"), row)
      val encoder = RowEncoder.apply(schema)
      gs.setSchema(schema)
      val rds = mu.mapPartitions(gs, encoder.asInstanceOf[Encoder[org.gorpipe.gor.model.Row]])
      if(nor) rds.drop("ChromNOR","PosNOR").map(row => new SparkRow(row).asInstanceOf[org.gorpipe.gor.model.Row])(SparkGOR.gorrowEncoder) else rds
    } else {
      val gs = new GorSpark(header, nor, SparkGOR.gorrowEncoder.schema, cmd, sgs.getProjectContext.getRoot)
      dsr.mapPartitions(gs, SparkGOR.gorrowEncoder)
    }
  }

  def header(): String = ds.schema.fieldNames.mkString("\t")

  def header(cmd: String, header: String): String = {
    val pf = new HeaderFunction(cmd, header)
    ds.asInstanceOf[Dataset[String]].mapPartitions(pf, Encoders.STRING).head()
  }

  def headerInfo(cmd: String, header: String): String = {
    val ghif = new GorHeaderInferFunction()
    val pf = new HeaderInferFunction(cmd, header)
    ds.asInstanceOf[Dataset[String]].mapPartitions(pf, Encoders.STRING).reduce(ghif)
  }

  def infer(): org.gorpipe.gor.model.Row = {
    val gr = new GorSparkRowInferFunction()
    val cn = tag.runtimeClass.getName
    val dsr = if (cn.equals("org.apache.spark.sql.Row")) {
      ds.asInstanceOf[Dataset[Row]].map(row => new GorSparkRow(row))(ds.asInstanceOf[Dataset[GorSparkRow]].encoder).asInstanceOf[Dataset[org.gorpipe.gor.model.Row]]
    } else if (cn.equals("java.lang.String")) {
      ds.asInstanceOf[Dataset[String]].map(row => RowObj(row))(SparkGOR.gorrowEncoder)
    } else {
      ds.asInstanceOf[Dataset[org.gorpipe.gor.model.Row]]
    }
    dsr.reduce(gr)
  }

  def inferSchema(header: String): StructType = {
    val row = infer()
    SparkRowUtilities.gor2Schema(header, row)
  }

  def inferEncoder(header: String): Encoder[org.gorpipe.gor.model.Row] = {
    val sc = inferSchema(header)
    RowEncoder(sc).asInstanceOf[Encoder[org.gorpipe.gor.model.Row]]
  }
}

object GorDatasetFunctions {
  implicit def addCustomFunctions[T: ClassTag](ds: Dataset[T]): GorDatasetFunctions[T] = new GorDatasetFunctions(ds)
}

class GorpipeRDD[T: ClassTag](prev: RDD[T], pipeStep: Analysis, encoder: ExpressionEncoder[T], header: String, gor: Boolean) extends RDD[T](prev) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val rowit = firstParent[T].iterator(split, context)
    val rs = if (gor) new GenomicIteratorBase {
      override def hasNext: Boolean = rowit.hasNext

      override def next(): org.gorpipe.gor.model.Row = {
        val r = rowit.next()
        r match {
          case row: Row => new GorSparkRow(row)
          case _ => RowObj.apply(r.toString)
        }
      }

      override def close(): Unit = {}

      override def seek(seekChr: String, seekPos: Int): Boolean = ???
    } else new GenomicIteratorBase {
      override def hasNext: Boolean = rowit.hasNext

      override def next(): org.gorpipe.gor.model.Row = {
        val r = rowit.next()
        r match {
          case row: Row => new SparkRow(row)
          case _ => RowObj.apply("chrN\t0\t" + r.toString)
        }
      }

      override def close(): Unit = {}

      override def seek(seekChr: String, seekPos: Int): Boolean = ???
    }
    rs.setHeader(if (gor) header else "chromNOR\tposNOR\t" + header)

    val bpsia = new BatchedPipeStepIteratorAdaptor(rs, pipeStep, rs.getHeader, GorPipe.brsConfig)
    new Iterator[T] {
      override def hasNext: Boolean = bpsia.hasNext

      override def next(): T = {
        val nspl = bpsia.next.toString.split("\t")
        val objs = new Array[Object](nspl.length)
        var i = 0
        while (i < nspl.length) {
          val dataTypes = encoder.schema.fields
          if (dataTypes(i).dataType eq IntegerType) objs(i) = nspl(i).toInt.asInstanceOf[Object]
          else if (dataTypes(i).dataType eq DoubleType) objs(i) = nspl(i).toDouble.asInstanceOf[Object]
          else objs(i) = nspl(i)

          i += 1
        }
        RowFactory.create(objs).asInstanceOf[T]
      }
    }
  }

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions
}

class TestGorRDD(prev: RDD[Row], gorcmd: String, header: String, gor: Boolean) extends RDD[org.gorpipe.gor.model.Row](prev) {
  override def compute(split: Partition, context: TaskContext): Iterator[org.gorpipe.gor.model.Row] = {
    val rowit = firstParent[Row].iterator(split, context)
    val rs = if (gor) new GenomicIteratorBase {
      override def hasNext: Boolean = rowit.hasNext

      override def next(): org.gorpipe.gor.model.Row = {
        val r = rowit.next()
        r match {
          case row: Row => new GorSparkRow(row)
          case _ => RowObj.apply(r.toString)
        }
      }

      override def close(): Unit = {}

      override def seek(seekChr: String, seekPos: Int): Boolean = ???
    } else new GenomicIteratorBase {
      override def hasNext: Boolean = rowit.hasNext

      override def next(): org.gorpipe.gor.model.Row = {
        val r = rowit.next()
        r match {
          case row: Row => new SparkRow(row)
          case _ => RowObj.apply("chrN\t0\t" + r.toString)
        }
      }

      override def close(): Unit = {}

      override def seek(seekChr: String, seekPos: Int): Boolean = ???
    }

    val gp = Paths.get("/gorproject")
    val gsf = if (Files.exists(gp)) new GenericSessionFactory(gp.toString, "result_cache") else new GenericSessionFactory
    val gps = gsf.create
    val pi = new PipeInstance(gps.getGorContext)
    val args = Array[String](gorcmd, "-stdin")
    val options = new PipeOptions
    options.parseOptions(args)
    pi.subProcessArguments(options)
    val an = pi.thePipeStep

    val bpsia = new BatchedPipeStepIteratorAdaptor(rs, an, header, GorPipe.brsConfig)
    new Iterator[org.gorpipe.gor.model.Row] {
      override def hasNext: Boolean = bpsia.hasNext

      override def next(): org.gorpipe.gor.model.Row = {
        val ret = bpsia.next
        ret
      }
    }
  }

  override protected def getPartitions: Array[Partition] = firstParent[Row].partitions
}

class GorRDD[T: ClassTag](prev: RDD[T], gorcmd: String, header: String, gor: Boolean) extends RDD[org.gorpipe.gor.model.Row](prev) {
  // override compute method to calculate the discount
  override def compute(split: Partition, context: TaskContext): Iterator[org.gorpipe.gor.model.Row] = {
    val rowit = firstParent[Row].iterator(split, context)
    val rs = if (gor) new GenomicIteratorBase {
      override def hasNext: Boolean = rowit.hasNext

      override def next(): org.gorpipe.gor.model.Row = {
        val r = rowit.next()
        r match {
          case row: org.gorpipe.gor.model.Row => row
          case _ => new GorSparkRow(r)
        }
      }

      override def close(): Unit = {}

      override def seek(seekChr: String, seekPos: Int): Boolean = ???
    } else new GenomicIteratorBase {
      override def hasNext: Boolean = rowit.hasNext

      override def next(): org.gorpipe.gor.model.Row = {
        val r = rowit.next()
        r match {
          case row: org.gorpipe.gor.model.Row => row
          case _ => new SparkRow(r)
        }
      }

      override def close(): Unit = {}

      override def seek(seekChr: String, seekPos: Int): Boolean = ???
    }

    val newheader = if (gor) header else "chromNOR\tposNOR\t" + header
    val gp = Paths.get("/gorproject")
    val gsf = if (Files.exists(gp)) new GenericSessionFactory(gp.toString, "result_cache") else new GenericSessionFactory
    val gps = gsf.create
    val pi = new PipeInstance(gps.getGorContext)
    pi.init(gorcmd, true, newheader)

    val bpsia = new BatchedPipeStepIteratorAdaptor(rs, pi.thePipeStep, newheader, GorPipe.brsConfig)
    new Iterator[org.gorpipe.gor.model.Row] {
      override def hasNext: Boolean = bpsia.hasNext

      override def next(): org.gorpipe.gor.model.Row = bpsia.next
    }
  }

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions
}

class QueryRDD(private val sparkSession: SparkSession, private val sqlContext: SQLContext /*, gorPipeSession: GorPipeSession**/ , commandsToExecute: Array[String], commandSignatures: Array[String], header: Boolean, projectDirectory: String, cacheDir: String, hash: String /*FileCache*/) extends RDD[String](sparkSession.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val i = split.index
    val (commandSignature, commandToExecute) = (commandSignatures(i), commandsToExecute(i))
    var cacheFile: String = null
    var overheadTime = 0
    val cacheDirectory = if (cacheDir != null) cacheDir else "result_cache"
    if (cacheFile == null) {
      val startTime = System.currentTimeMillis()
      log.debug("CacheFile=" + cacheFile)
      val commandUpper = commandToExecute.toUpperCase

      if (commandToExecute.startsWith("gordict")) {
        if (hash != null) {
          //cacheFile = hash.tempLocation(commandSignature, ".gord")
        } else {
          cacheFile = cacheDirectory + "/" + commandSignature + ".dict.gord"
        }
      } else {
        if (commandUpper.startsWith("SELECT ") || commandUpper.startsWith("SPARK ") || commandUpper.startsWith("GORSPARK ") || commandUpper.startsWith("NORSPARK")) {
          cacheFile = if (header) cacheDirectory + "/" + commandSignature + ".header.parquet" else cacheDirectory + "/" + commandSignature + ".parquet"
        } else if (commandUpper.startsWith("CMD -N ")) {
          cacheFile = if (header) cacheDirectory + "/" + commandSignature + ".header.txt" else cacheDirectory + "/" + commandSignature + ".txt"
        } else if (commandToExecute.toUpperCase.startsWith("NOR ")) {
          cacheFile = if (header) cacheDirectory + "/" + commandSignature + ".header.nor" else cacheDirectory + "/" + commandSignature + ".tsv"
        } else {
          cacheFile = if (header) cacheDirectory + "/" + commandSignature + ".header.gor" else cacheDirectory + "/" + commandSignature + ".gorz"
        }
      }
      log.debug("CacheFile decided at {}", cacheFile)

      // Do this if we have result cache active or if we are running locally and the local cacheFile does not exist.
      val projectPath = Paths.get(projectDirectory)
      val path = new org.apache.hadoop.fs.Path(cacheFile)
      val cpath = projectPath.resolve(cacheFile)
      val md5 = hash != null
      if (md5 || !Files.exists(cpath)) {
        // We are using absolute paths here
        val startTime = System.currentTimeMillis
        var extension: String = null
        if (commandToExecute.startsWith("gordictpart")) {
          overheadTime = 1000 * 60 * 10 // 10 minutes
          val w = commandToExecute.split(" ")
          var dictFiles: List[String] = Nil
          var partitions: List[String] = Nil
          var i = 1
          while (i < w.length - 1) {
            dictFiles ::= GeneralQueryHandler.getRelativeFileLocationForDictionaryFileReferences(w(i))
            partitions ::= w(i + 1)
            i += 2
          }
          val dictList = dictFiles.zip(partitions).map(x => {
            val f = x._1
            val part = x._2
            // file, alias
            f + "\t" + part
          })
          AnalysisUtilities.writeList(cpath, dictList)
          extension = ".gord"
        } else if (commandToExecute.startsWith("gordict")) {
          overheadTime = 1000 * 60 * 10 // 10 minutes
          val w = commandToExecute.split(" ")
          var dictFiles: List[String] = Nil
          var chromsrange: List[String] = Nil
          var i = 1
          while (i < w.length - 1) {
            dictFiles ::= GeneralQueryHandler.getRelativeFileLocationForDictionaryFileReferences(w(i))
            chromsrange ::= w(i + 1)
            i += 2
          }
          var chrI = 0
          val dictList = dictFiles.zip(chromsrange).map(x => {
            val f = x._1
            val cep = x._2.split(":")
            val stasto = cep(1).split("-")
            val (c, sp, ep) = (cep(0), stasto(0), stasto(1))
            chrI += 1
            // file, alias, chrom, startpos, chrom, endpos
            f + "\t" + chrI + "\t" + c + "\t" + sp + "\t" + c + "\t" + ep
          })
          AnalysisUtilities.writeList(cpath, dictList)
          extension = ".gord"
        } else {
          val temp_cacheFile = AnalysisUtilities.getTempFileName(cacheFile)
          val parquet = if (temp_cacheFile.endsWith(""".parquet""")) projectPath.resolve(temp_cacheFile).toAbsolutePath.toString else null
          val sessionFactory = new GenericSessionFactory(projectDirectory, null)
          val gorPipeSession = sessionFactory.create() //new GorSession("")
          DynIterator.createGorIterator = (gorContext: GorContext) => {
            PipeInstance.createGorIterator(gorContext)
          }
          val theSource = new DynamicRowSource(commandToExecute, gorPipeSession.getGorContext, true)
          val theHeader = theSource.getHeader

          val oldName = projectPath.resolve(temp_cacheFile)
          try {
            val nor = theSource.isNor
            val grc = if (gorPipeSession.getSystemContext.getRunnerFactory != null) gorPipeSession.getSystemContext.getRunnerFactory else new GenericRunnerFactory()
            val runner = grc.create()
            runner.run(theSource, if (parquet != null) null else OutFile(oldName.toAbsolutePath.normalize().toString, theHeader, skipHeader = false, columnCompress = nor, nor = true, md5 = true, md5File = true, GorIndexType.NONE, Option.empty))
            Files.move(oldName, cpath)
          } catch {
            case e: Exception =>
              try {
                Files.delete(oldName)
              } catch {
                case _: Exception => /* do nothing */
              }
              throw e
          }
          if (commandUpper.startsWith("CMD -N ")) {
            extension = if (header) ".txt" else ".txt"
          } else if (commandUpper.startsWith("NOR ")) {
            extension = if (header) ".nor" else ".nor"
          } else {
            extension = if (header) ".gor" else ".gorz"
          }

        }
      }
      log.debug("Query handler execution time " + (System.currentTimeMillis() - startTime))
    }
    Iterator(cacheFile)
  }

  override protected def getPartitions: Array[Partition] = {
    val plist = ListBuffer[Partition]()
    var i = 0
    while (i < commandsToExecute.length) {
      plist += new Partition {
        val p: Int = i

        override def index: Int = {
          p
        }
      }
      i += 1
    }
    plist.toArray
  }
}

class RowGorRDD(@transient private val sparkSession: SparkSession, gorcmd: String, header: String, filter: String, chr: String, pos: Int, end: Int, gor: Boolean) extends PgorRDD[org.gorpipe.gor.model.Row](sparkSession, gorcmd, header, filter, chr, pos, end, gor)

class PgorRDD[T: ClassTag](@transient private val sparkSession: SparkSession, gorcmd: String, header: String, filter: String, chr: String, pos: Int, end: Int, gor: Boolean) extends RDD[org.gorpipe.gor.model.Row](sparkSession.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[org.gorpipe.gor.model.Row] = {

    var newgorcmd = gorcmd
    if (filter != null && filter.length > 0) {
      val i = split.index
      val fsel = filter.split(",")(i)
      newgorcmd = newgorcmd.substring(0, 4) + "-f" + fsel + newgorcmd.substring(3)
    }
    val args = Array[String](newgorcmd)
    val pi = new PipeInstance(null)
    pi.processArguments(args, executeNor = false)
    val it = pi.getIterator
    new Iterator[org.gorpipe.gor.model.Row]() {
      override def hasNext: Boolean = {
        val ret = it.hasNext
        if (!ret) it.close()
        ret
      }

      override def next(): org.gorpipe.gor.model.Row = it.next()
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val plist = ListBuffer[Partition]()
    if (filter != null && filter.length > 0) {
      var i = 0
      val s = filter.split(",")
      while (i < s.length) {
        plist += new Partition {
          val p: Int = i

          override def index: Int = {
            p
          }
        }
        i += 1
      }
    } else {
      val p = new Partition {
        override def index: Int = 0
      }
      plist += p
    }
    plist.toArray
  }
}

object sparkGorTest {

  case class Person(name: String, age: Long) {
    override def toString: String = name + "\t" + age
  }

  def testSparkGor(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val g = new PgorRDD[String](spark, "gor", "more", null, null, 0, -1, true)
    g.foreach(f => System.err.println(f))
  }
}

case class Gorz(CHROM: String, POS: Int, block: String)

case class GorzStr(CHROM: String, POS: String, block: String)

case class Result(CHROM: String, POS: Int, REF: String, ALT: String, ID: String, M: String)

object SparkGOR {

  import org.apache.spark.sql.Encoders
  import org.apache.spark.sql.types.StructType

  def bool(b: scala.Boolean): scala.Boolean = b

  def me(map: Map[String, String]): Map[String, String] = {
    val mutableMap = scala.collection.mutable.Map[String, String](map.toSeq: _*)
    mutableMap.put("header", "true")
    mutableMap.toMap
  }

  val gorzSchema = ScalaReflection.schemaFor[Gorz].dataType.asInstanceOf[StructType]

  case class Skat(chrom: String, start: Int, stop: Int, tag: String, numMarkers: Int, pvalue: String)

  case class Gene(chr: String, pos: Int, end: Int, Gene_Symbol: String)

  case class Dbsnp(CHROM: String, POS: Int, REF: String, ALT: String, ID: String)

  val gorzFlatMap = new GorzFlatMap
  val gorrowEncoder: Encoder[org.gorpipe.gor.model.Row] = Encoders.javaSerialization(classOf[org.gorpipe.gor.model.Row])
  val rowbaseEncoder: Encoder[RowBase] = Encoders.javaSerialization(classOf[org.gorpipe.gor.model.RowBase])
  val gorsparkrowbaseEncoder: Encoder[GorSparkRowBase] = Encoders.javaSerialization(classOf[GorSparkRowBase])
  val sparkrowEncoder: Encoder[SparkRow] = Encoders.javaSerialization(classOf[SparkRow])
  val gorSparkrowEncoder: Encoder[GorSparkRow] = Encoders.javaSerialization(classOf[GorSparkRow])
  val gorzIterator = new GorzIterator()

  case class Variants(chrom: String, pos: Int, ref: String, alt: String, cc: Int, cr: Double, depth: Int, gl: Int, filter: String, fs: Double, formatZip: String, pn: String)

  def where(w: String, schema: StructType): GorRowFilterFunction[org.gorpipe.gor.model.Row] = {
    new GorSparkRowFilterFunction[org.gorpipe.gor.model.Row](w, schema)
  }

  def where(w: String, header: Array[String], gortypes: Array[String]): GorRowFilterFunction[org.gorpipe.gor.model.Row] = {
    new GorRowFilterFunction[org.gorpipe.gor.model.Row](w, header, gortypes)
  }

  def calc(c: String, header: Array[String], gortypes: Array[String]): GorRowMapFunction = {
    new GorRowMapFunction(c, header, gortypes)
  }

  def analyze(q: String): GorSparkRowQueryFunction = {
    new GorSparkRowQueryFunction(q)
  }

  def query(q: String, header: Array[String]): GorSpark = {
    new GorSpark(null, false, null, q, null)
  }

  def createSession(sparkSession: SparkSession, root: String, cache: String, gorconfig: String, goralias: String): GorSparkSession = {
    val standalone = System.getProperty("sm.standalone")
    if (standalone == null || standalone.length == 0) System.setProperty("sm.standalone", root)
    val sessionFactory = new SparkSessionFactory(sparkSession, root, cache, gorconfig, goralias, null)
    val sparkGorSession = sessionFactory.create().asInstanceOf[GorSparkSession]
    sparkGorSession
  }

  def createSession(sparkSession: SparkSession): GorSparkSession = {
    createSession(sparkSession, Paths.get("").toAbsolutePath.toString, Paths.get(System.getProperty("java.io.tmpdir")).toString, null, null)
  }

  def createSession(sparkSession: SparkSession, gorconfig: String, goralias: String): GorSparkSession = {
    createSession(sparkSession, Paths.get("").toAbsolutePath.toString, Paths.get(System.getProperty("java.io.tmpdir")).toString, gorconfig, goralias)
  }
}
