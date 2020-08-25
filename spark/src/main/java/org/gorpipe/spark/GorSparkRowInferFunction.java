package org.gorpipe.spark;

import org.apache.spark.api.java.function.ReduceFunction;
import org.gorpipe.gor.function.GorRowInferFunction;
import org.gorpipe.gor.model.Row;

public class GorSparkRowInferFunction extends GorRowInferFunction implements ReduceFunction<Row> {
    @Override
    public Row call(Row row, Row t1) {
        return apply(row,t1);
    }
}

