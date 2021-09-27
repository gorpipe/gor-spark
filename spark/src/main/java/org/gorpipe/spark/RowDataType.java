package org.gorpipe.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public class RowDataType {
    public Dataset<? extends Row> dataset;
    public DataType[]   datatypes;
    public Instant  timestamp;

    public RowDataType(Dataset<? extends Row> dataset, DataType[] datatypes) {
        this.dataset = dataset;
        this.datatypes = datatypes;
        this.timestamp = Instant.now();
    }

    public boolean isAfter(Path p) throws IOException {
        return p != null && timestamp.isAfter(Files.getLastModifiedTime(p).toInstant());
    }
}