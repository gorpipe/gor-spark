package gorsat.spark;

import org.apache.spark.sql.connector.read.partitioning.Partitioning;

public class GorPartitioning implements Partitioning {

    @Override
    public int numPartitions() {
        return 2;
    }
}
