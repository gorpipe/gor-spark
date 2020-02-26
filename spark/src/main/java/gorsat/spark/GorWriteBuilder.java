package gorsat.spark;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

public abstract class GorWriteBuilder implements WriteBuilder {

    @Override
    public BatchWrite buildForBatch() {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support batch write");
    }

    @Override
    public StreamingWrite buildForStreaming() {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support streaming write");
    }
}
