package org.gorpipe.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.function.GorRowFilterFunction;
import org.gorpipe.model.genome.files.gor.Row;

import java.util.Arrays;

public class GorSparkRowFilterFunction<T extends Row> extends GorRowFilterFunction<Row> implements FilterFunction<T> {
    public GorSparkRowFilterFunction(String gorwhere, String[] header, String[] types) {
        super(gorwhere, header, types);
    }

    public GorSparkRowFilterFunction(String gorwhere, StructType schema) {
        this(gorwhere, schema.fieldNames(), gorTypes(schema));
    }

    static String[] gorTypes(StructType schema) {
        return Arrays.stream(schema.fields()).map(StructField::dataType).map(d -> {
            if(d.sameType(DataTypes.IntegerType)) return "I";
            else if(d.sameType(DataTypes.DoubleType)) return "D";
            else return "S";
        }).toArray(String[]::new);
    }

    @Override
    public boolean call(T value) {
        return test(value);
    }
}
