package gorsat.process;

import gorsat.BatchedPipeStepIteratorAdaptor;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.model.genome.files.gor.Row;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GorSparkMaterialize extends GorSpark {
    int max;

    public GorSparkMaterialize(String inputHeader, boolean nor, StructType schema, String gorcmd, String gorroot, int max) {
        super(inputHeader, nor, schema, gorcmd, gorroot);
        this.max = max;
    }

    public GorSparkMaterialize(String inputHeader, boolean nor, StructType schema, String gorcmd, String gorroot, String uri, String jobId, int max) {
        super(inputHeader,nor,schema,gorcmd,gorroot,uri,jobId);
        this.max = max;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> iterator) {
        BatchedPipeStepIteratorAdaptor bpia = getIterator(iterator);
        List<Row> res = StreamSupport.stream(bpia, false).limit(max).collect(Collectors.toList());
        bpia.close();
        return res.stream().iterator();
    }
}
