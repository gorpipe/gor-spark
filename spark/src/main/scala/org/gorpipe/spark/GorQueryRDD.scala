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
class GorQueryRDD(sparkSession: SparkSession, commandsToExecute: Array[String], commandSignatures: Array[String], cacheFiles: Array[String], projectDirectory: String, cacheDirectory: String, configFile: String, aliasFile: String, jobIds: Array[String], secCtxs: Array[String]) extends RDD[String](sparkSession.sparkContext, Nil) {
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
    val (jobId, commandSignature, commandToExecute, cacheFile, securityContext) = (jobIds(i), commandSignatures(i), commandsToExecute(i), cacheFiles(i), if (secCtxs != null) secCtxs(i) else null)
    log.debug("CacheFile=" + cacheFile)

    // Do this if we have result cache active or if we are running locally and the local cacheFile does not exist.
    val projectPath = Paths.get(projectDirectory)
    val cacheFilePath = Paths.get(cacheFile)
    val absCacheFilePath = if( projectDirectory.nonEmpty && !cacheFilePath.isAbsolute ) projectPath.resolve(cacheFile) else cacheFilePath
    val cacheFileMd5Path = absCacheFilePath.getParent.resolve(absCacheFilePath.getFileName+".md5")
    val cacheFileMetaPath = absCacheFilePath.getParent.resolve(absCacheFilePath.getFileName+".meta")
    if(Files.isDirectory(absCacheFilePath)) {
      val sparkGorMonitor : GorMonitor = if(SparkGorMonitor.monitorFactory!=null) SparkGorMonitor.monitorFactory.createSparkGorMonitor(jobId) else null
      val engine = new SparkGorExecutionEngine(commandToExecute, projectDirectory, cacheDirectory, configFile, aliasFile, cacheFilePath, securityContext, sparkGorMonitor)
      engine.execute()
    } else if (!Files.exists(absCacheFilePath)) {
      val temp_cacheFile = if(absCacheFilePath.getFileName.toString.startsWith("tmp")) absCacheFilePath else absCacheFilePath.getParent.resolve("tmp"+UUID.randomUUID()+absCacheFilePath.getFileName.toString)
      val temp_cacheMd5File = temp_cacheFile.getParent.resolve(temp_cacheFile.getFileName.toString+".md5")
      val temp_cacheMetaFile = temp_cacheFile.getParent.resolve(temp_cacheFile.getFileName.toString+".meta")
      val tempFile_absolutepath = temp_cacheFile.toAbsolutePath.normalize()
      try {
        val sparkGorMonitor : GorMonitor = if(SparkGorMonitor.monitorFactory!=null) SparkGorMonitor.monitorFactory.createSparkGorMonitor(jobId) else null
        val engine = new SparkGorExecutionEngine(commandToExecute, projectDirectory, cacheDirectory, configFile, aliasFile, tempFile_absolutepath, securityContext, sparkGorMonitor)
        engine.execute()
        if (!Files.exists(absCacheFilePath)) {
          Files.move(temp_cacheFile, absCacheFilePath)
          if(Files.exists(temp_cacheMd5File) && !Files.exists(cacheFileMd5Path)) Files.move(temp_cacheMd5File, cacheFileMd5Path)
          if(Files.exists(temp_cacheMetaFile) && !Files.exists(cacheFileMetaPath)) Files.move(temp_cacheMetaFile, cacheFileMetaPath)
        }
      } catch {
        case e: Exception => handleException(e, temp_cacheFile)
      }
    }
    val relCachePath = if(projectDirectory.nonEmpty && cacheFilePath.isAbsolute) {
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