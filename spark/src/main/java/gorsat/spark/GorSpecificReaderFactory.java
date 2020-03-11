package gorsat.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class GorSpecificReaderFactory implements PartitionReaderFactory {

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        GorSpecificInputPartition p = (GorSpecificInputPartition) partition;
        return new PartitionReader<InternalRow>() {
            private int current = -1;

            @Override
            public boolean next() {
                current += 1;
                return current < p.i.length;
            }

            @Override
            public InternalRow get() {
                return new GenericInternalRow(new Object[]{p.i[current], p.j[current]});
            }

            @Override
            public void close() {

            }
        };
    }
}
