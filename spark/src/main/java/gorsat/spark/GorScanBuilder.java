package gorsat.spark;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;

public abstract class GorScanBuilder implements ScanBuilder, Scan, Batch, SupportsPushDownFilters {
    StructType schema;
    String redisUri;
    String jobId;
    String cacheFile;
    String useCpp;
    String projectRoot;
    String cacheDir;

    public GorScanBuilder(StructType schema, String redisUri, String jobId, String cacheFile, String projectRoot, String cacheDir, String useCpp) {
        this.schema = schema;
        this.redisUri = redisUri;
        this.jobId = jobId;
        this.cacheFile = cacheFile;
        this.useCpp = useCpp;
        this.projectRoot = projectRoot;
        this.cacheDir = cacheDir;
    }

    @Override
    public Scan build() {
        return this;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new GorReaderFactory(schema, redisUri, jobId, cacheFile, projectRoot, cacheDir, useCpp);
    }
}
