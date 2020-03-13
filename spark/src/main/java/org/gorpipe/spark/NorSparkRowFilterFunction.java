package com.nextcode.gor.spark;

import org.apache.spark.sql.Row;

public class NorSparkRowFilterFunction extends GorFilterFunction {
    public NorSparkRowFilterFunction(String gorwhere, String[] header, String[] types) {
        super(gorwhere, header, types);
    }

    @Override
    public boolean call(Row value) {
        return test(new SparkRow(value));
    }
}
