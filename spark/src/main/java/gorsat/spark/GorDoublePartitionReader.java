package gorsat.spark;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public class GorDoublePartitionReader extends GorPartitionReader {
    Double doubleRow;

    public GorDoublePartitionReader(StructType schema, GorRangeInputPartition gorRangeInputPartition, String redisUri, String jobId, String projectRoot, String cacheDir, String configFile, String aliasFile, String securityContext, String useCpp) {
        super(schema,gorRangeInputPartition,redisUri,jobId,projectRoot,cacheDir,configFile,aliasFile,securityContext,useCpp);
    }

    @Override
    public boolean next() {
        if( iterator == null ) {
            initIterator();
        }
        boolean hasNext = iterator.hasNext();
        if( hasNext ) {
            org.gorpipe.gor.model.Row gorrow = iterator.next();
            doubleRow = gorrow.colAsDouble(2);
        }
        return hasNext;
    }

    @Override
    public InternalRow get() {
        return serializer.apply(RowFactory.create(doubleRow));
    }

    @Override
    public void close() {
        if(iterator != null) iterator.close();
    }
}
