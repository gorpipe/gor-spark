package gorsat.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.immutable.Map
import java.io.IOException
import java.util.zip.DataFormatException


class GorDataSource extends FileDataSourceV2 with RelationProvider with SchemaRelationProvider { //TableProvider, RelationProvider, SchemaRelationProvider, DataSourceRegister {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    try {
      val batchTable = new GorBatchTable(options.get("query"), options.getBoolean("tag", false), options.get("path"), options.get("f"), options.get("ff"), options.get("s"), options.get("split"), options.get("p"), options.get("redis"), options.get("jobid"), options.get("cachefile"), options.get("securityContext"), options.get("native"), options.getBoolean("hadoop",false)) {}
      val projectroot = options.get("projectroot")
      if (projectroot != null) batchTable.setProjectRoot(projectroot)
      val cachedir = options.get("cachedir")
      if (cachedir != null) batchTable.setCacheDir(cachedir)
      batchTable.setConfigFile(options.get("configfile"))
      batchTable.setAliasFile(options.get("aliasfile"))
      return batchTable
    } catch {
      case e@(_: IOException | _: DataFormatException) =>
        e.printStackTrace()
    }
    null
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val batchTable = new GorBatchTable(options.get("query"), options.getBoolean("tag", false), options.get("path"), options.get("f"), options.get("ff"), options.get("s"), options.get("split"), options.get("p"), schema, options.get("redis"), options.get("jobid"), options.get("cachefile"), options.get("securityContext"), options.get("native"), options.getBoolean("hadoop",false)) {}
    val projectroot = options.get("projectroot")
    if (projectroot != null) batchTable.setProjectRoot(projectroot)
    val cachedir = options.get("cachedir")
    if (cachedir != null) batchTable.setCacheDir(cachedir)
    batchTable.setConfigFile(options.get("configfile"))
    batchTable.setAliasFile(options.get("aliasfile"))
    batchTable
  }

  override def createRelation(theSqlContext: SQLContext, parameters: Map[String, String], theschema: StructType): BaseRelation = new BaseRelation() {
    override def schema: StructType = theschema

    override
    def sqlContext: SQLContext = theSqlContext
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = null

  override def shortName = "gor"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[GorFileFormat]

  override def getPaths(map: CaseInsensitiveStringMap): Seq[String] = null //super.getPaths(map);
}
