package gorsat.process;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.StreamSupport;
import gorsat.BatchedPipeStepIteratorAdaptor;
import gorsat.Commands.Analysis;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.GorSession;
import org.gorpipe.model.genome.files.gor.GorMonitor;
import org.gorpipe.model.genome.files.gor.Row;
import org.gorpipe.spark.SparkGorMonitor;
import org.gorpipe.spark.SparkGorRow;

public class GorSpark implements MapPartitionsFunction<Row, Row> {
    StructType schema;
    boolean nor;
    String header;
    String gorcmd;
    String gorroot;
    String uri;
    String jobId;

    public GorSpark(String inputHeader, boolean nor, StructType schema, String gorcmd, String gorroot) {
        this.schema = schema;
        this.nor = nor;
        this.header = inputHeader;
        this.gorcmd = gorcmd;
        this.gorroot = gorroot;
    }

    public GorSpark(String inputHeader, boolean nor, StructType schema, String gorcmd, String gorroot, String uri, String jobId) {
        this(inputHeader,nor,schema,gorcmd,gorroot);
        this.uri = uri;
        this.jobId = jobId;
    }

    public void setSchema(StructType st) {
        schema = st;
    }

    public PipeInstance query() {
        String[] args = {gorcmd,"-stdin"};
        PipeOptions pipeOptions = new PipeOptions();
        pipeOptions.parseOptions(args);

        pipeOptions.gorRoot_$eq(gorroot);
        Path projectPath = Paths.get(gorroot);

        GenericSessionFactory gsf = Files.exists(projectPath) ? new GenericSessionFactory(gorroot, "result_cache") : new GenericSessionFactory();
        GorSession gps = gsf.create();
        gps.setNorContext(nor);

        if( uri != null ) {
            GorMonitor gorMonitor = new SparkGorMonitor(uri, jobId);
            gps.getSystemContext().setMonitor(gorMonitor);
        }

        PipeInstance pi = new PipeInstance(gps.getGorContext());
        pi.init(pipeOptions.query(), pipeOptions.stdIn(), header/*, pipeOptions.jobId(), pipeOptions.schema()*/);
        return pi;
    }

    BatchedPipeStepIteratorAdaptor getIterator(Iterator<? extends Row> iterator) {
        PipeInstance pi = query();
        Analysis an = pi.getPipeStep();
        return new BatchedPipeStepIteratorAdaptor(iterator, an, header, GorPipe.brsConfig());
    }

    @Override
    public Iterator<Row> call(Iterator<Row> iterator) {
        //return getIterator(iterator);
        return StreamSupport.stream(getIterator(iterator),false).map(r -> (Row)new SparkGorRow(r,schema)).iterator();
    }
}
