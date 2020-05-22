package gorsat.InputSources

import java.io.File
import java.nio.file.{Files, Paths}

import org.gorpipe.spark.GorSparkSession
import gorsat.Commands.CommandParseUtilities._
import gorsat.Commands.{CommandArguments, GenomicRange, InputSourceInfo, InputSourceParsingResult}
import gorsat.process.{GorJavaUtilities, SparkRowSource}
import org.gorpipe.gor.GorContext

object Spark {

  private def processAllArguments(context: GorContext, argString: String, iargs: Array[String], args: Array[String], isNorContext: Boolean): InputSourceParsingResult = {
    val range = if (hasOption(args, "-p")) rangeOfOption(args, "-p") else GenomicRange.empty
    val jobId = stringValueOfOptionWithDefault(args,"-j","-1")
    val fi = iargs.indexOf("-ff")
    val command = if (iargs.indexOf("-ff") >= 0) {
      replaceSingleQuotes(iargs.slice(fi+2,iargs.length).mkString(" "))
    } else replaceSingleQuotes(iargs.mkString(" "))
    val usegorpipe = hasOption(args, "-g")

    if (command.contains(".yml?")) {
      var e = command.indexOf(' ')
      if (e == -1) e = command.length
      //val qr = YamlUtils.parseYaml(command.substring(0, e), gpSession)
      //command = qr + command.substring(e)
    }
    else if (command.contains(".yml")) {
      val e = command.indexOf(')')
      //if (e == -1) command = YamlUtils.parseYaml(command, gpSession)
      //else command = YamlUtils.parseYaml(command.substring(0, e + 1), gpSession) + command.substring(e + 1)
    }

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
    var tag = hasOption(args, "-tag")
    val native = hasOption(args, "-c")

    val myCommand = GorJavaUtilities.seekReplacement(command, range.chromosome, range.start, range.stop)
    val inputSource = new SparkRowSource(myCommand, null, null, isNorContext, gpSession.asInstanceOf[GorSparkSession], filter, filterFile, filterColumn, splitFile, range.chromosome, range.start, range.stop, usegorpipe, jobId, native, parts, buckets, tag)
    InputSourceParsingResult(inputSource, inputSource.getHeader, isNorContext)
  }

  class Spark() extends InputSourceInfo("SPARK", CommandArguments("-n -c -tag", "-p -s -g -f -j -ff -split -buck -part", 1, -1, ignoreIllegalArguments = true)) {

    override def processArguments(context: GorContext, argString: String, iargs: Array[String],
                                  args: Array[String]): InputSourceParsingResult = {

      processAllArguments(context, argString, iargs, args, hasOption(args, "-n"))
    }
  }

  class GorSpark() extends InputSourceInfo("GORSPARK", CommandArguments("-c -tag", "-p -s -g -f -j -ff -split", 1, -1, ignoreIllegalArguments = true)) {

    override def processArguments(context: GorContext, argString: String, iargs: Array[String],
                                  args: Array[String]): InputSourceParsingResult = {

      processAllArguments(context, argString, iargs, args, isNorContext = false)
    }
  }

  class NorSpark() extends InputSourceInfo("NORSPARK", CommandArguments("-c -tag", "-p -s -g -f -j -ff -split", 1, -1, ignoreIllegalArguments = true)) {

    override def processArguments(context: GorContext, argString: String, iargs: Array[String],
                                  args: Array[String]): InputSourceParsingResult = {

      processAllArguments(context, argString, iargs, args, isNorContext = true)
    }
  }

}