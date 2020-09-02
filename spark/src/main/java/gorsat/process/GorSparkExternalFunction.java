package gorsat.process;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.gorpipe.gor.model.Line;
import org.gorpipe.gor.model.Row;
import org.gorpipe.gor.model.RowBase;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.model.gor.RowObj;

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
    
    public GorSparkExternalFunction(String header, String cmd, String goroot) {
        this.header = header;
        this.cmd = cmd;
        this.goroot = goroot;
    }

    public void setFetchHeader(boolean fetchHeader) {
        this.fetchHeader = fetchHeader;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) throws Exception {
        GenericSessionFactory gsf = goroot != null && Files.exists(Paths.get(goroot)) ? new GenericSessionFactory(goroot, "result_cache") : new GenericSessionFactory();
        GorSession gps = gsf.create();

        ProcessIteratorAdaptor it = new ProcessIteratorAdaptor(gps.getGorContext(), cmd, "", input, null, header, false, Optional.empty(), false,false);
        if(fetchHeader) {
            String rowstr = it.getHeader();
            int[] sa = RowObj.splitArray(rowstr);
            Row gorrow = new RowBase("chrN", 0, rowstr, sa, null);
            return Collections.singletonList(gorrow).iterator();
        } else {
            return it;
        }
    }
}
