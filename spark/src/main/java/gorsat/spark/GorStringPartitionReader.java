package gorsat.spark;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.model.gor.RowObj;

public class GorStringPartitionReader extends GorPartitionReader {
    String stringRow;

    public GorStringPartitionReader(StructType schema, GorRangeInputPartition gorRangeInputPartition, String redisUri, String jobId, String projectRoot, String cacheDir, String useCpp) {
        super(schema,gorRangeInputPartition,redisUri,jobId,projectRoot,cacheDir,useCpp);
    }

    @Override
    public boolean next() {
        if( iterator == null ) {
            initIterator();
        }
        boolean hasNext = iterator.hasNext();
        if( hasNext ) {
            org.gorpipe.gor.model.Row gorrow = iterator.next();
            if (nor) {
                stringRow = gorrow.otherCols();
            } else {
                if (p.tag != null) gorrow = gorrow.rowWithAddedColumn(p.tag);
                hasNext = p.chr == null || (gorrow.chr.equals(p.chr) && (p.end == -1 || gorrow.pos <= p.end));
                stringRow = gorrow.toString();
            }
        }
        return hasNext;
    }

    @Override
    public InternalRow get() {
        return serializer.apply(RowFactory.create(stringRow));
    }

    @Override
    public void close() {
        if(iterator != null) iterator.close();
    }
}
