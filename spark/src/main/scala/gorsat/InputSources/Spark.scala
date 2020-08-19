package gorsat.InputSources

import java.io.File
import java.nio.file.{Files, Paths}

import org.gorpipe.spark.{GorSparkSession, SparkOperatorRunner}
import gorsat.Commands.CommandParseUtilities._
import gorsat.Commands.{CommandArguments, GenomicRange, InputSourceInfo, InputSourceParsingResult}
import gorsat.Iterators.RowListIterator
import gorsat.process.{GorJavaUtilities, SparkRowSource}
import org.gorpipe.gor.GorContext
import org.gorpipe.model.gor.RowObj

object Spark {

  private def processAllArguments(context: GorContext, iargs: Array[String], args: Array[String], isNorContext: Boolean): InputSourceParsingResult = {
    val range = if (hasOption(args, "-p")) rangeOfOption(args, "-p") else GenomicRange.empty
    val jobId = stringValueOfOptionWithDefault(args,"-j","-1")
    val fi = iargs.indexOf("-ff")
    val command = if (iargs.indexOf("-ff") >= 0) {
      replaceSingleQuotes(iargs.slice(fi+2,iargs.length).mkString(" "))
    } else replaceSingleQuotes(iargs.mkString(" "))
    val usegorpipe = hasOption(args, "-g")

    val inputSource = if (command.contains(".yml?")) {
      var e = command.indexOf(' ')
      if (e == -1) e = command.length
      val lRows = List(RowObj("chr1\t1"))
      RowListIterator(lRows)
      //val qr = YamlUtils.parseYaml(command.substring(0, e), gpSession)
      //command = qr + command.substring(e)
    } else if (command.contains(".yml")) {
      val qr = context.getSession.getSystemContext.getReportBuilder.parse(command)
      val sparkOperatorRunner = new SparkOperatorRunner()
      sparkOperatorRunner.run(qr,context.getSession.getProjectContext.getRoot)
      val lRows = List(RowObj("chr1\t1"))
      RowListIterator(lRows)
    } else {
      val gpSession = context.getSession
      var projectRoot = gpSession.getProjectContext.getRoot.split("[ \t]+")(0)
      if (projectRoot != null && projectRoot.length > 0) {
        val rootPath = Paths.get(projectRoot)
        if (Files.exists(rootPath)) {
          val rootRealPath = rootPath.toRealPath()
          projectRoot = rootRealPath.toString
        }
      } else projectRoot = new File(".").getAbsolutePath

      val selectedColumns = stringValueOfOptionWithDefault(args, "-a", null)
      val filter = stringValueOfOptionWithDefault(args, "-f", null)
      val filterFile = stringValueOfOptionWithDefault(args, "-ff", null)
      val filterColumn = stringValueOfOptionWithDefault(args, "-s", null)
      val splitFile = stringValueOfOptionWithDefault(args, "-split", null)
      val buckets = intValueOfOptionWithDefault(args, "-buck", -1)
      val parts = stringValueOfOptionWithDefault(args, "-part", null)
      val tag = hasOption(args, "-tag")
      val native = hasOption(args, "-c")

      val myCommand = GorJavaUtilities.seekReplacement(command, range.chromosome, range.start, range.stop)
      new SparkRowSource(myCommand, null, null, isNorContext, gpSession.asInstanceOf[GorSparkSession], filter, filterFile, filterColumn, splitFile, range.chromosome, range.start, range.stop, usegorpipe, jobId, native, parts, buckets, tag)
    }
    InputSourceParsingResult(inputSource, inputSource.getHeader, isNorContext)
  }

  class Select() extends InputSourceInfo("SELECT", CommandArguments("-n -c -tag", "-p -s -g -f -j -ff -split -buck -part", 1, -1, ignoreIllegalArguments = true)) {

    override def processArguments(context: GorContext, argString: String, iargs: Array[String],
                                  args: Array[String]): InputSourceParsingResult = {

      val niargs = "select" +: iargs
      processAllArguments(context, niargs, args, hasOption(args, "-n"))
    }
  }

  class Spark() extends InputSourceInfo("SPARK", CommandArguments("-n -c -tag", "-p -s -g -f -j -ff -split -buck -part", 1, -1, ignoreIllegalArguments = true)) {

    override def processArguments(context: GorContext, argString: String, iargs: Array[String],
                                  args: Array[String]): InputSourceParsingResult = {

      processAllArguments(context, iargs, args, hasOption(args, "-n"))
    }
  }

  class GorSpark() extends InputSourceInfo("GORSPARK", CommandArguments("-c -tag", "-p -s -g -f -j -ff -split", 1, -1, ignoreIllegalArguments = true)) {

    override def processArguments(context: GorContext, argString: String, iargs: Array[String],
                                  args: Array[String]): InputSourceParsingResult = {

      processAllArguments(context, iargs, args, isNorContext = false)
    }
  }

  class NorSpark() extends InputSourceInfo("NORSPARK", CommandArguments("-c -tag", "-p -s -g -f -j -ff -split", 1, -1, ignoreIllegalArguments = true)) {

    override def processArguments(context: GorContext, argString: String, iargs: Array[String],
                                  args: Array[String]): InputSourceParsingResult = {

      processAllArguments(context, iargs, args, isNorContext = true)
    }
  }

}