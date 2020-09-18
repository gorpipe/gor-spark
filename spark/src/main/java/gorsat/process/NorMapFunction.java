package gorsat.process;

import org.gorpipe.spark.SparkRow;

import java.util.OptionalInt;

public class NorMapFunction extends GorMapFunction {
    NorMapFunction(FilterParams filterParams, OptionalInt replaceIndex) {
        super(filterParams, replaceIndex);
    }

    @Override
    public org.apache.spark.sql.Row call(org.apache.spark.sql.Row row) {
        Object[] lobj = replaceIndex == -1 ? new Object[row.size() + 1] : new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            lobj[i] = row.get(i);
        }
        SparkRow cvp = new SparkRow(row);
        lobj[replaceIndex == -1 ? row.size() : replaceIndex] = func != null ? func.apply(cvp) : "";
        return org.apache.spark.sql.RowFactory.create(lobj);
    }
}
