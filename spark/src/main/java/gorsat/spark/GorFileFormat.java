package gorsat.spark;

import gorsat.process.SparkRowUtilities;
import org.apache.hadoop.fs.FileSystem;
import org.gorpipe.gor.model.DriverBackedFileReader;
import org.gorpipe.spark.SparkGOR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.zip.DataFormatException;

public class GorFileFormat extends CSVFileFormat implements Serializable {
    @Override
    public Option<StructType> inferSchema(SparkSession session, Map<String, String> options, Seq<FileStatus> files) {
        String pathstr = options.get("path").get();
        Option<String> osec = options.get("securityContext");
        String securityContext = osec.isDefined() ? osec.get() : "";
        Option<String> ohadoop = options.get("hadoop");
        boolean hadoopInfer = ohadoop.isDefined() && Boolean.parseBoolean(ohadoop.get());
        StructType ret = null;
        try {
            InputStream is;
            if(hadoopInfer) {
                Path path = new Path(pathstr);
                Configuration conf = new Configuration();
                //conf.set("fs.s3a.endpoint","localhost:4566");
                conf.set("fs.s3a.connection.ssl.enabled","false");
                conf.set("fs.s3a.path.style.access","true");
                conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
                conf.set("fs.s3a.change.detection.mode","warn");
                conf.set("com.amazonaws.services.s3.enableV4","true");
                conf.set("fs.s3a.committer.name","partitioned");
                conf.set("fs.s3a.committer.staging.conflict-mode","replace");
                conf.set("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore");
                conf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
                FileSystem fs = path.getFileSystem(conf);
                is = fs.open(path);
            } else {
                var fileReader = new DriverBackedFileReader(securityContext, "/", new Object[]{});
                is = fileReader.getInputStream(pathstr);
            }
            ret = SparkRowUtilities.inferSchema(is, pathstr, false, pathstr.endsWith(".gorz"));
        } catch (IOException | DataFormatException e) {
            e.printStackTrace();
        }
        return Option.apply(ret);
    }

    @Override
    public OutputWriterFactory prepareWrite(SparkSession sparkSession, Job job, Map<String, String> options, StructType dataSchema) {
        return new OutputWriterFactory() {
            @Override
            public OutputWriter newInstance(String path, StructType dataSchema, TaskAttemptContext context) {
                try {
                    return new GorOutputWriter(path, dataSchema, options.get("path").get());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public String getFileExtension(TaskAttemptContext context) {
                return CodecConfig.from(context).getCodec().getExtension() + ".gorz";
            }
        };
    }

    @Override
    public boolean supportBatch(SparkSession sparkSession, StructType dataSchema) {
        return super.supportBatch(sparkSession, dataSchema);
    }

    @Override
    public Option<Seq<String>> vectorTypes(StructType requiredSchema, StructType partitionSchema, SQLConf sqlConf) {
        return super.vectorTypes(requiredSchema, partitionSchema, sqlConf);
    }

    @Override
    public boolean isSplitable(SparkSession sparkSession, Map<String, String> options, Path path) {
        return super.isSplitable(sparkSession, options, path);
    }

    @Override
    public Function1<PartitionedFile, Iterator<InternalRow>> buildReader(SparkSession sparkSession, StructType dataSchema, StructType partitionSchema, StructType requiredSchema, Seq<Filter> filters, Map<String, String> options, Configuration hadoopConf) {
        Function1<PartitionedFile, Iterator<InternalRow>> func;

        String pathstr = options.get("path").get();
        boolean isGorz = pathstr.endsWith(".gorz");
        boolean isGord = pathstr.endsWith(".gord");

        if( isGord ) {
            func = new GordFunction(requiredSchema);
        } else {
            Map<String, String> soptions = SparkGOR.me(options);
            func = super.buildReader(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, soptions, hadoopConf);

            if (isGorz) {
                //var lgattr = JavaConverters.asJavaCollection(requiredSchema.toAttributes()).stream().map(Attribute::toAttribute).collect(Collectors.toList());
                //var sgattr = JavaConverters.asScalaBuffer(lgattr).toSeq();
                //ExpressionEncoder gorzencoder = SparkGOR.gorzencoder().resolveAndBind(sgattr, SimpleAnalyzer$.MODULE$);
                return new GorzFunction(func, requiredSchema, JavaConverters.asJavaCollection(filters));
            }
        }
        return func;
    }

    @Override
    public Function1<PartitionedFile, Iterator<InternalRow>> buildReaderWithPartitionValues(SparkSession sparkSession, StructType dataSchema, StructType partitionSchema, StructType requiredSchema, Seq<Filter> filters, Map<String, String> options, Configuration hadoopConf) {
        return super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf);
    }

    @Override
    public boolean supportDataType(DataType dataType) {
        return true;
    }

    @Override
    public String shortName() {
        return "gor";
    }
}
