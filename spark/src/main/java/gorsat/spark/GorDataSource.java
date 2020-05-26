package gorsat.spark;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.streaming.ui.BatchTableBase;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.util.zip.DataFormatException;

public class GorDataSource implements FileDataSourceV2, RelationProvider, SchemaRelationProvider { //TableProvider, RelationProvider, SchemaRelationProvider, DataSourceRegister {
    @Override
    public Table getTable(CaseInsensitiveStringMap options) {
        try {
            GorBatchTable batchTable = new GorBatchTable(options.get("query"), options.getBoolean("tag", false), options.get("path"), options.get("f"), options.get("ff"), options.get("s"), options.get("split"), options.get("p"), options.get("redis"), options.get("jobid"), options.get("cachefile"), options.get("native")) {};
            String projectroot = options.get("projectroot");
            if(projectroot!=null) batchTable.setProjectRoot(projectroot);
            String cachedir = options.get("cachedir");
            if(cachedir!=null) batchTable.setCacheDir(cachedir);
            return batchTable;
        } catch (IOException | DataFormatException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Table getTable(CaseInsensitiveStringMap options, StructType schema) {
        GorBatchTable batchTable = new GorBatchTable(options.get("query"), options.getBoolean("tag", false), options.get("path"), options.get("f"), options.get("ff"), options.get("s"), options.get("split"), options.get("p"), schema, options.get("redis"), options.get("jobid"), options.get("cachefile"), options.get("native")) {};
        String projectroot = options.get("projectroot");
        if(projectroot!=null) batchTable.setProjectRoot(projectroot);
        String cachedir = options.get("cachedir");
        if(cachedir!=null) batchTable.setCacheDir(cachedir);
        return batchTable;
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters, StructType schema) {
        return new BaseRelation() {
            @Override
            public StructType schema() {
                return schema;
            }

            @Override
            public SQLContext sqlContext() {
                return sqlContext;
            }
        };
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        return null;
    }

    @Override
    public String shortName() {
        return "gor";
    }

    @Override
    public Class<? extends FileFormat> fallbackFileFormat() {
        return GorFileFormat.class;
    }

    @Override
    public Seq<String> getPaths(CaseInsensitiveStringMap map) {
        return null; //super.getPaths(map);
    }

    @Override
    public String getTableName(Seq<String> paths) {
        return shortName() + paths.mkString("_");
    }

    @Override
    public SparkSession sparkSession() {
        return SparkSession.active();
    }
}
