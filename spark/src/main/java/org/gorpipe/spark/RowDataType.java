package org.gorpipe.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.time.Instant;
import java.util.List;

public class RowDataType {
    public Dataset<? extends Row> dataset;
    public DataType[]   datatypes;
    public List<Instant>  timestamp;
    public String path;

    public RowDataType(Dataset<? extends Row> dataset, DataType[] datatypes, String path, List<Instant> inst) {
        this.dataset = dataset;
        this.datatypes = datatypes;
        this.timestamp = inst;
        this.path = path;
    }

    public RowDataType(String path, List<Instant> inst) {
        this.timestamp = inst;
        this.path = path;
    }

    public List<Instant> getTimestamp() {
        return timestamp;
    }
}