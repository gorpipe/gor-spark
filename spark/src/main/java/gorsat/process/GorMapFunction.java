package gorsat.process;

import gorsat.parser.ParseArith;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.gorpipe.spark.GorSparkRow;
import scala.Function1;

import java.io.Serializable;
import java.util.OptionalInt;

public class GorMapFunction implements MapFunction<Row, Row>, Serializable {
    transient ParseArith filter;
    String calcType;
    Function1 func;
    int replaceIndex;

    GorMapFunction(FilterParams filterParams, OptionalInt rIdx) {
        filter = new ParseArith(null);
        filter.setColumnNamesAndTypes(filterParams.headersplit, filterParams.colType);
        calcType = filter.compileCalculation(filterParams.paramString);
        replaceIndex = rIdx.isPresent() ? rIdx.getAsInt() : -1;

        if (calcType.equals("String")) func = filter.stringFunction();
        else if (calcType.equals("Double")) func = filter.doubleFunction();
        else if (calcType.equals("Long")) func = filter.longFunction();
        else if (calcType.equals("Int")) func = filter.intFunction();
        else if (calcType.equals("Boolean")) func = filter.booleanFunction();
    }

    public String getCalcType() {
        return calcType;
    }

    @Override
    public org.apache.spark.sql.Row call(org.apache.spark.sql.Row row) {
        Object[] lobj = replaceIndex == -1 ? new Object[row.size() + 1] : new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            lobj[i] = row.get(i);
        }
        GorSparkRow cvp = new GorSparkRow(row);
        lobj[replaceIndex == -1 ? row.size() : replaceIndex] = func != null ? func.apply(cvp) : "";
        return org.apache.spark.sql.RowFactory.create(lobj);
    }
}
