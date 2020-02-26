package gorsat.spark;

import org.apache.spark.sql.connector.read.partitioning.ClusteredDistribution;
import org.apache.spark.sql.connector.read.partitioning.Distribution;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;

import java.util.Arrays;

public class GorPartitioning implements Partitioning {

    @Override
    public int numPartitions() {
        return 2;
    }

    @Override
    public boolean satisfy(Distribution distribution) {
        if (distribution instanceof ClusteredDistribution) {
            String[] clusteredCols = ((ClusteredDistribution) distribution).clusteredColumns;
            return Arrays.asList(clusteredCols).contains("i");
        }

        return false;
    }
}
