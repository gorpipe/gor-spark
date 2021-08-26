package gorsat.spark;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class GorSpecificScanBuilder extends GorScanBuilder implements SupportsReportPartitioning {
    public GorSpecificScanBuilder(StructType schema, String redisUri, String jobId, String cacheFile, String projectRoot, String cacheDir, String configFile, String aliasFile, String securityContext, String useCpp) {
        super(schema, redisUri, jobId, cacheFile, projectRoot, cacheDir, configFile, aliasFile, securityContext, useCpp);
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] partitions = new InputPartition[2];
        partitions[0] = new GorSpecificInputPartition(new int[]{1, 1, 3}, new int[]{4, 4, 6});
        partitions[1] = new GorSpecificInputPartition(new int[]{2, 4, 4}, new int[]{6, 2, 2});
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new GorSpecificReaderFactory();
    }

    @Override
    public Partitioning outputPartitioning() {
        return new GorPartitioning();
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        return new Filter[0];
    }

    @Override
    public Filter[] pushedFilters() {
        return new Filter[0];
    }
}
