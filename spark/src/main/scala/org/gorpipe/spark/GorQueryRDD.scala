package org.gorpipe.spark

import java.nio.file.{Files, Path, Paths}

import com.nextcode.gor.spark.{GorPartition, SparkGorExecutionEngine, SparkGorMonitor}
import gorsat.AnalysisUtilities
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ListBuffer

//Todo handle GORDICT and GORDICTPART
class GorQueryRDD(sparkSession: SparkSession, commandsToExecute: Array[String], commandSignatures: Array[String], cacheFiles: Array[String], projectDirectory: String, cacheDirectory: String, jobIds: Array[String], redisUri: String) extends RDD[String](sparkSession.sparkContext, Nil) {
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
    if (!Files.exists(cacheFilePath)) {
      val temp_cacheFileName = AnalysisUtilities.getTempFileName(cacheFile)
      var temp_cacheFile = Paths.get(temp_cacheFileName)
      if( !temp_cacheFile.isAbsolute ) temp_cacheFile = projectPath.resolve(temp_cacheFileName)
      val tempFile_absolutepath = temp_cacheFile.toAbsolutePath.normalize().toString
      try {
        val sparkGorMonitor = new SparkGorMonitor(redisUri, jobId)
        val engine = new SparkGorExecutionEngine(commandToExecute, projectDirectory, cacheDirectory, tempFile_absolutepath, sparkGorMonitor)
        engine.execute()
        Files.move(temp_cacheFile, cacheFilePath)
      } catch {
        case e: Exception => handleException(e, temp_cacheFile)
      }
    }
    Iterator(jobId+'\t'+commandSignature+'\t'+cacheFile)
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