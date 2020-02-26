package gorsat.spark;

import org.apache.spark.sql.connector.read.InputPartition;

class GorRangeInputPartition implements InputPartition {
    String path;
    String filterFile;
    String filter;
    String chr;
    int start;
    int end;
    String tag;

    GorRangeInputPartition(String path, String filter, String filterFile, String chr, int start, int end, String tag) {
        this.path = path;
        this.filterFile = filterFile;
        this.filter = filter;
        this.chr = chr;
        this.start = start;
        this.end = end;
        this.tag = tag;
    }
}