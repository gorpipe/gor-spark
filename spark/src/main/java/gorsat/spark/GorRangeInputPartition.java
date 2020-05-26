package gorsat.spark;

import org.apache.spark.sql.connector.read.InputPartition;

class GorRangeInputPartition implements InputPartition {
    String query;
    String path;
    String filterFile;
    String filter;
    String filterColumn;
    String chr;
    int start;
    int end;
    String tag;

    GorRangeInputPartition(String query, String tag) {
        this.query = query;
        this.tag = tag;
    }

    GorRangeInputPartition(String path, String filter, String filterFile, String filterColumn, String chr, int start, int end, String tag) {
        this.path = path;
        this.filterFile = filterFile;
        this.filter = filter;
        this.filterColumn = filterColumn;
        this.chr = chr;
        this.start = start;
        this.end = end;
        this.tag = tag;
    }
}