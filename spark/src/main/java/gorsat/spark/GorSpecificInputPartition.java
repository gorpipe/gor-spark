package gorsat.spark;

import org.apache.spark.sql.connector.read.InputPartition;

public class GorSpecificInputPartition implements InputPartition {
    int[] i;
    int[] j;

    GorSpecificInputPartition(int[] i, int[] j) {
        assert i.length == j.length;
        this.i = i;
        this.j = j;
    }
}