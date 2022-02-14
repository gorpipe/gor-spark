package gorsat.spark;

import gorsat.BatchedPipeStepIteratorAdaptor;
import gorsat.BatchedReadSourceConfig;
import gorsat.process.GenericSessionFactory;
import gorsat.process.GorPipe;
import gorsat.process.PipeInstance;
import gorsat.process.PipeOptions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.spark.ScalaUtils;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.spark.SparkGorRow;
import scala.Function1;
import scala.collection.Iterator;

import java.io.Serializable;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GordFunction implements Function1<PartitionedFile, Iterator<InternalRow>>, Serializable {
    String header;
    ExpressionEncoder<Row> encoder;
    ExpressionEncoder.Serializer<Row> serializer;

    public GordFunction(StructType schema) {
        this.header = String.join("\t",schema.fieldNames());
        var sattr = ScalaUtils.seqAttribute(schema.toAttributes());
        encoder = RowEncoder.apply(schema).resolveAndBind(sattr, SimpleAnalyzer$.MODULE$);
        serializer = encoder.createSerializer();
    }

    @Override
    public Iterator<InternalRow> apply(PartitionedFile v1) {
        GenericSessionFactory sessionFactory = new GenericSessionFactory("/gorproject","result_cache");
        GorSession gorPipeSession = sessionFactory.create();
        PipeInstance pi = new PipeInstance(gorPipeSession.getGorContext());

        String[] args = {v1.filePath()};
        PipeOptions options = new PipeOptions();
        options.parseOptions(args);
        pi.subProcessArguments(options);

        BatchedReadSourceConfig brsConfig = GorPipe.brsConfig();
        BatchedPipeStepIteratorAdaptor batchedPipeStepIteratorAdaptor = new BatchedPipeStepIteratorAdaptor(pi.getIterator(), pi.thePipeStep(), true, header, brsConfig);
        Stream<InternalRow> ir = StreamSupport.stream(batchedPipeStepIteratorAdaptor,false).map(r -> new SparkGorRow(r, encoder.schema())).map(r -> serializer.apply(r));

        return ScalaUtils.iterator(ir.iterator());
    }

    @Override
    public <A> Function1<A, Iterator<InternalRow>> compose(Function1<A, PartitionedFile> g) {
        return null; //super.compose(g);
    }

    @Override
    public <A> Function1<PartitionedFile, A> andThen(Function1<Iterator<InternalRow>, A> g) {
        return null; //super.andThen(g);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
