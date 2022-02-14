package gorsat.spark;

import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.types.StructType;

public abstract class GorScanBuilder implements ScanBuilder, Scan, Batch, SupportsPushDownFilters {
    StructType schema;
    String redisUri;
    String streamKey;
    String jobId;
    String cacheFile;
    String useCpp;
    String projectRoot;
    String cacheDir;
    String configFile;
    String aliasFile;
    String securityContext;

    public GorScanBuilder(StructType schema, String redisUri, String streamKey, String jobId, String cacheFile, String projectRoot, String cacheDir, String configFile, String aliasFile, String securityContext, String useCpp) {
        this.schema = schema;
        this.redisUri = redisUri;
        this.streamKey = streamKey;
        this.jobId = jobId;
        this.cacheFile = cacheFile;
        this.useCpp = useCpp;
        this.projectRoot = projectRoot;
        this.cacheDir = cacheDir;
        this.configFile = configFile;
        this.aliasFile = aliasFile;
        this.securityContext = securityContext;
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
        return new GorReaderFactory(schema, redisUri, streamKey, jobId, cacheFile, projectRoot, cacheDir, configFile, aliasFile, securityContext, useCpp);
    }
}
