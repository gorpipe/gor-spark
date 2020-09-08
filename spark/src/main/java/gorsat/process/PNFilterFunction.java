package gorsat.process;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PNFilterFunction implements FilterFunction<Row>, Serializable {
    Set<String> pns;
    int colnum;

    public PNFilterFunction(String filter, int colNum) {
        pns = new HashSet<>(Arrays.asList(filter.split(",")));
        colnum = colNum;
    }

    @Override
    public boolean call(org.apache.spark.sql.Row row) {
        String str = row.getString(colnum);
        return pns.contains(str);
    }
}
