package org.gorpipe.spark;

import java.util.Iterator;
import gorsat.Commands.Analysis;
import gorsat.process.PipeInstance;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.gorpipe.gor.function.GorRowQueryFunction;
import org.gorpipe.model.genome.files.gor.Row;

public class GorSparkRowQueryFunction extends GorRowQueryFunction implements FlatMapFunction<Row, Row> {
    public GorSparkRowQueryFunction(String query) {
        super(query);
    }

    @Override
    public void initAdaptor() {
        PipeInstance pi = init(header);
        Analysis pipeStep = pi.thePipeStep();
        lra = new ListSparkRowAdaptor(null);
        bufferedPipeStep = pipeStep != null ? pipeStep.$bar(lra) : lra;
    }

    @Override
    public Iterator<Row> call(Row row) {
        return apply(row).iterator();
    }
}
