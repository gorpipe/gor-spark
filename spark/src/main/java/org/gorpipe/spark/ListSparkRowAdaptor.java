package org.gorpipe.spark;

import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.function.ListRowAdaptor;
import org.gorpipe.gor.model.Row;

public class ListSparkRowAdaptor extends ListRowAdaptor {
    StructType schema;

    public ListSparkRowAdaptor(StructType schema) {
        this.schema = schema;
    }

    @Override
    public void process(Row r) {
        lr.add(r);
    }
}