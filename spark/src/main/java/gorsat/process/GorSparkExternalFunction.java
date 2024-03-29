package gorsat.process;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.model.Row;
import org.gorpipe.gor.model.RowBase;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.model.gor.RowObj;
import org.gorpipe.spark.SparkGorRow;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

public class GorSparkExternalFunction implements MapPartitionsFunction<Row, Row> {
    String header;
    String cmd;
    String goroot;
    boolean fetchHeader = false;
    StructType schema;
    
    public GorSparkExternalFunction(String header, String cmd, String goroot) {
        this.header = header;
        this.cmd = cmd;
        this.goroot = goroot;
    }

    public void setFetchHeader(boolean fetchHeader) {
        this.fetchHeader = fetchHeader;
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) throws Exception {
        GenericSessionFactory gsf = goroot != null && Files.exists(Paths.get(goroot)) ? new GenericSessionFactory(goroot, "result_cache") : new GenericSessionFactory();
        GorSession gps = gsf.create();

        ProcessIteratorAdaptor it = new ProcessIteratorAdaptor(gps.getGorContext(), cmd, "", input, null, header, false, Optional.empty(), false,false);
        if(fetchHeader) {
            String rowstr = it.getHeader();
            it.close();
            int[] sa = RowObj.splitArray(rowstr);
            Row gorrow = new RowBase("chrN", 0, rowstr, sa, null);
            return Collections.singletonList(gorrow).iterator();
        } else {
            return new Iterator<>() {
                Row last;
                boolean closed = false;

                @Override
                public boolean hasNext() {
                    if (it.hasNext()) {
                        last = new SparkGorRow(it.next(),schema);
                        return true;
                    } else if (!closed) {
                        it.close();
                        closed = true;
                    }
                    return false;
                }

                @Override
                public Row next() {
                    if (last == null) hasNext();
                    return last;
                }
            };
        }
    }
}
