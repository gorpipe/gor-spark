package org.gorpipe.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partition, TaskContext}
import org.gorpipe.gor.model.{DriverBackedFileReader, FileReader}
import org.gorpipe.gor.table.util.PathUtils

import java.net.URI
import java.util.UUID
import scala.collection.mutable.ListBuffer

//Todo handle GORDICT and GORDICTPART
class GorQueryRDD(sparkSession: SparkSession, commandsToExecute: Array[String], commandSignatures: Array[String], cacheFiles: Array[String], projectDirectory: String, cacheDirectory: String, configFile: String, aliasFile: String, jobIds: Array[String], secCtxs: Array[String], redisUri: String, redisKey: String) extends RDD[String](sparkSession.sparkContext, Nil) {
  require(cacheDirectory!=null)
  require(projectDirectory!=null)
  require(commandsToExecute!=null)
  require(commandSignatures!=null)
  require(cacheFiles!=null)
  require(commandsToExecute.length==cacheFiles.length)

  def handleException(e: Exception, fileReader: FileReader, temp_cacheFile: String): Unit = {
      try {
        fileReader.delete(temp_cacheFile)
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
    val opts = new Array[Object](0)
    val fileReader = new DriverBackedFileReader(securityContext, projectDirectory, opts)
    val projectPath = projectDirectory
    val cacheFilePath = cacheFile
    val absCacheFilePath = PathUtils.resolve(projectDirectory,cacheFile)
    //val cacheFileMd5Path = absCacheFilePath.getParent.resolve(absCacheFilePath.getFileName+".md5")
    val cacheFileMetaPath = absCacheFilePath+".meta"
    if(fileReader.isDirectory(absCacheFilePath)) {
      val sparkGorMonitor = GorSparkUtilities.getSparkGorMonitor(jobId, redisUri, redisKey)
      val engine = new SparkGorExecutionEngine(commandToExecute, projectDirectory, cacheDirectory, configFile, aliasFile, cacheFilePath, securityContext, sparkGorMonitor)
      engine.execute()
    } else if (!fileReader.exists(absCacheFilePath)) {
      val absCacheFileName = PathUtils.getFileName(absCacheFilePath)
      val temp_cacheFile = if(absCacheFileName.startsWith("tmp")) absCacheFilePath else PathUtils.resolve(PathUtils.getParent(absCacheFilePath),"tmp"+UUID.randomUUID()+absCacheFileName)
      //val temp_cacheMd5File = temp_cacheFile.getParent.resolve(temp_cacheFile.getFileName.toString+".md5")
      val temp_cacheMetaFile = temp_cacheFile+".meta"
      val tempFile_absolutepath = PathUtils.resolve(projectDirectory,temp_cacheFile)
      try {
        val sparkGorMonitor = GorSparkUtilities.getSparkGorMonitor(jobId, redisUri, redisKey)
        val engine = new SparkGorExecutionEngine(commandToExecute, projectDirectory, cacheDirectory, configFile, aliasFile, tempFile_absolutepath, securityContext, sparkGorMonitor)
        engine.execute()
        if (!fileReader.exists(absCacheFilePath)) {
          fileReader.move(temp_cacheFile, absCacheFilePath)
          //if(Files.exists(temp_cacheMd5File) && !Files.exists(cacheFileMd5Path)) Files.move(temp_cacheMd5File, cacheFileMd5Path)
          if(fileReader.exists(temp_cacheMetaFile) && !fileReader.exists(cacheFileMetaPath)) fileReader.move(temp_cacheMetaFile, cacheFileMetaPath)
        }
      } catch {
        case e: Exception => handleException(e, fileReader, temp_cacheFile)
      }
    }
    val relCachePath = if(projectDirectory.nonEmpty && PathUtils.isAbsolutePath(cacheFilePath)) {
      PathUtils.relativize(URI.create(projectDirectory),cacheFilePath)
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