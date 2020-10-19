package gorsat.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class GorReaderFactory implements PartitionReaderFactory {
    StructType schema;
    String redisUri;
    String jobId;
    String cacheFile;
    String useCpp;
    String projectRoot;
    String cacheDir;
    String configFile;
    String aliasFile;

    public GorReaderFactory(StructType schema, String redisUri, String jobId, String cacheFile, String projectRoot, String cacheDir, String configFile, String aliasFile, String useCpp) {
        this.schema = schema;
        this.redisUri = redisUri;
        this.jobId = jobId;
        this.cacheFile = cacheFile;
        this.useCpp = useCpp;
        this.projectRoot = projectRoot;
        this.cacheDir = cacheDir;
        this.configFile = configFile;
        this.aliasFile = aliasFile;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        StructField[] fields = schema.fields();
        GorRangeInputPartition p = (GorRangeInputPartition) partition;
        PartitionReader<InternalRow> partitionReader;
        if(useCpp != null && useCpp.equalsIgnoreCase("blue")) {
            partitionReader = new NativePartitionReader(fields,p);
        } else if(fields.length>1) {
            partitionReader = new GorPartitionReader(schema,p,redisUri,jobId,projectRoot,cacheDir,configFile,aliasFile,useCpp);
        } else {
            partitionReader = new GorStringPartitionReader(schema,p,redisUri,jobId,projectRoot,cacheDir,configFile,aliasFile,useCpp);
        }
        return partitionReader;
    }
}