package org.gorpipe.spark

import java.nio.file.{Files, Path, Paths}
import gorsat.Utilities.AnalysisUtilities
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partition, TaskContext}
import org.gorpipe.gor.monitor.GorMonitor

import java.util.UUID
import scala.collection.mutable.ListBuffer

//Todo handle GORDICT and GORDICTPART
class GorQueryRDD(sparkSession: SparkSession, commandsToExecute: Array[String], commandSignatures: Array[String], cacheFiles: Array[String], projectDirectory: String, cacheDirectory: String, configFile: String, aliasFile: String, jobIds: Array[String], redisUri: String) extends RDD[String](sparkSession.sparkContext, Nil) {
  require(cacheDirectory!=null)
  require(projectDirectory!=null)
  require(commandsToExecute!=null)
  require(commandSignatures!=null)
  require(cacheFiles!=null)
  require(commandsToExecute.length==cacheFiles.length)

  def handleException(e: Exception, temp_cacheFile: Path): Unit = {
      try {
        Files.delete(temp_cacheFile)
      } catch {
        case ee: Exception => /* do nothing */
      }
      throw e
  }

  //val cluster : GorClusterBase = null
  override def compute(partition: Partition, context: TaskContext): Iterator[String] = {
    val i = partition.index
    val (jobId, commandSignature, commandToExecute, cacheFile) = (jobIds(i), commandSignatures(i), commandsToExecute(i), cacheFiles(i))
    log.debug("CacheFile=" + cacheFile)

    // Do this if we have result cache active or if we are running locally and the local cacheFile does not exist.
    val projectPath = Paths.get(projectDirectory)
    var cacheFilePath = Paths.get(cacheFile)
    if( !cacheFilePath.isAbsolute ) cacheFilePath = projectPath.resolve(cacheFile)
    val cacheFileMd5Path = cacheFilePath.getParent.resolve(cacheFilePath.getFileName+".md5")
    if (!Files.exists(cacheFilePath)) {
      val temp_cacheFile = if(cacheFilePath.getFileName.toString.startsWith("tmp")) cacheFilePath else cacheFilePath.getParent.resolve("tmp"+UUID.randomUUID()+cacheFilePath.getFileName.toString)
      val temp_cacheMd5File = temp_cacheFile.getParent.resolve(temp_cacheFile.getFileName.toString+".md5")
      val tempFile_absolutepath = temp_cacheFile.toAbsolutePath.normalize().toString
      try {
        val sparkGorMonitor : GorMonitor = if(SparkGorMonitor.localProgressMonitor!=null) SparkGorMonitor.localProgressMonitor else new SparkGorMonitor(redisUri, jobId)
        val engine = new SparkGorExecutionEngine(commandToExecute, projectDirectory, cacheDirectory, configFile, aliasFile, tempFile_absolutepath, sparkGorMonitor)
        engine.execute()
        Files.move(temp_cacheFile, cacheFilePath)
        if(Files.exists(temp_cacheMd5File)) Files.move(temp_cacheMd5File, cacheFileMd5Path)
      } catch {
        case e: Exception => handleException(e, temp_cacheFile)
      }
    }
    val relCachePath = if(cacheFilePath.isAbsolute) {
      projectPath.relativize(cacheFilePath).toString
    } else {
      cacheFile
    }
    Iterator(jobId+'\t'+commandSignature+'\t'+relCachePath)
  }

  override def collect: Array[String] = {
    super.collect()
  }

  override protected def getPartitions: Array[Partition] = {
    val plist = ListBuffer[Partition]()
    var i = 0
    while (i < commandsToExecute.length) {
      plist += new GorPartition(i)
      i += 1
    }
    plist.toArray
  }
}