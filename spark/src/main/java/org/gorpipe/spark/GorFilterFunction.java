package org.gorpipe.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.gorpipe.gor.function.GorRowFilterFunction;

public class GorFilterFunction extends GorRowFilterFunction<org.gorpipe.gor.model.Row> implements FilterFunction<Row> {
    public GorFilterFunction(String gorwhere, String[] header, String[] gortypes) {
        super(gorwhere, header, gortypes);
    }

    @Override
    public boolean call(Row value) {
        return test(new GorSparkRow(value));
    }
}
