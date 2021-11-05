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
import org.gorpipe.gor.model.Row;
import org.gorpipe.gor.monitor.GorMonitor;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.spark.GorSparkUtilities;
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
        Path projectPath = Paths.get(gorroot);

        GenericSessionFactory gsf = Files.exists(projectPath) ? new GenericSessionFactory(gorroot, "result_cache") : new GenericSessionFactory();
        GorSession gps = gsf.create();
        gps.setNorContext(nor);

        if( uri != null ) {
            GorMonitor gorMonitor = GorSparkUtilities.getSparkGorMonitor(jobId, uri);
            gps.getSystemContext().setMonitor(gorMonitor);
        }

        PipeInstance pi = new PipeInstance(gps.getGorContext());
        pi.init(gorcmd, true, header);
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
