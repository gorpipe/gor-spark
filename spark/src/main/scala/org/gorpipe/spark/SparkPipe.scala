package org.gorpipe.spark

import gorsat._
import gorsat.process.{GorPipeFirstOrderCommands, PipeOptions}
import org.gorpipe.base.config.ConfigManager
import org.gorpipe.exceptions.{ExceptionUtilities, GorException}
import org.gorpipe.gor.model.{DbSource, DefaultFileReader}
import org.gorpipe.gor.servers.GorConfig
import org.gorpipe.gor.util.ConfigUtil
import org.slf4j.LoggerFactory

import scala.language.postfixOps

object SparkPipe extends GorPipeFirstOrderCommands {
  private val consoleLogger = LoggerFactory.getLogger("console." + this.getClass)

  var version: String = getClass.getPackage.getImplementationVersion
  if (version eq null) {
    version = "Unknown"
  }

  val brsConfig: BatchedReadSourceConfig = ConfigManager.createPrefixConfig("gor", classOf[BatchedReadSourceConfig])
  val gorConfig: GorConfig = ConfigManager.createPrefixConfig("gor", classOf[GorConfig])

  /**
   * Main definition accepts an argument string and ensures database sources are initialized.
   */
  def main(args: Array[String]) {
    //"spark ho.yml(name=sim,projectroot=/mnt/csa/env/dev/projects/ukbb_hg38,freeze=freeze/ukbb_500k/array_vep95p1,variants=freeze/ukbb_500k/array_vep95p1/metadata/AF.gorz,pnlist=user_data/simmi/pns4.txt,partsize=1000,pcacomponents=3,outfile=user_data/simmi/sko.nor)"
    // Display help
    if (args.length < 1 || args(0).isEmpty || args(0).toUpperCase.startsWith("HELP")) {
      helpCommand(args, new DefaultFileReader(""))
      System.exit(0)
    }

    // Parse the input parameters
    val commandlineOptions = new PipeOptions
    commandlineOptions.parseOptions(args)

    ExceptionUtilities.setShowStackTrace(commandlineOptions .showStackTrace)

    if (commandlineOptions.version) {
      printOutGORPipeVersion()
      System.exit(0)
    }

    // Initialize config
    ConfigUtil.loadConfig("gor")

    // Initialize database connections
    DbSource.initInConsoleApp()

    var exitCode = 0
    //todo find a better way to construct

    val sparkGorRedisUri = GorSparkUtilities.getSparkGorRedisUri()
    val sparkMonitor = new SparkGorMonitor(sparkGorRedisUri,"-1")
    val executionEngine = new SparkGorExecutionEngine(commandlineOptions.query, commandlineOptions.gorRoot, commandlineOptions.cacheDir, null, null,null, sparkMonitor)

    try {
      executionEngine.execute()
    } catch {
      case ge: GorException =>
        consoleLogger.error(ExceptionUtilities.gorExceptionToString(ge))
        exitCode = -1
      case ex: Throwable =>
        consoleLogger.error("Unexpected error, please report if you see this.\n" + ex.getMessage, ex)
        exitCode = -1
    }

    //System.exit(exitCode)
  }

  private def printOutGORPipeVersion(): Unit = {
    val version = getClass.getPackage.getImplementationVersion
    System.out.println(if (version != null) version else "No implementation version found.")
  }
}
