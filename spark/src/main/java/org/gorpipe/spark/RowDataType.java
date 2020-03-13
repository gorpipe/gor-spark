package org.gorpipe.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

public class RowDataType {
    public Dataset<? extends Row> dataset;
    public DataType[]   datatypes;

    public RowDataType(Dataset<? extends Row> dataset, DataType[] datatypes) {
        this.dataset = dataset;
        this.datatypes = datatypes;
    }
}