package org.gorpipe.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.function.GorRowMapFunction;
import org.gorpipe.model.genome.files.gor.Row;

import java.util.Arrays;
import java.util.stream.IntStream;

public class GorSparkRowMapFunction extends GorRowMapFunction implements MapFunction<Row,Row> {
    StructType schema;

    public GorSparkRowMapFunction(String name, String query, StructType schema) {
        super(query, schema.fieldNames(), Arrays.stream(schema.fields()).map(StructField::dataType).map(d -> {
            if(d.sameType(DataTypes.IntegerType)) return "I";
            else if(d.sameType(DataTypes.DoubleType)) return "D";
            else return "S";
        }).toArray(String[]::new));
        this.schema = deriveSchema(name, schema);
    }

    public GorSparkRowMapFunction(String name, String query, String[] header, String[] types) {
        super(query, header, types);
        this.schema = deriveSchema(name, header, types);
    }

    public StructType getSchema() {
        return schema;
    }

    private StructType deriveSchema(String name, String[] header, String[] types) {
        StructField[] fields = new StructField[header.length+1];
        IntStream.range(0, types.length).forEach(i -> {
            if(types[i].equals("I")) fields[i] = new StructField(name, DataTypes.IntegerType, true, Metadata.empty());
            else if(types[i].equals("D")) fields[i] = new StructField(name, DataTypes.DoubleType, true, Metadata.empty());
            else fields[i] = new StructField(name, DataTypes.StringType, true, Metadata.empty());
        });
        if( calcType.equals("Int") ) fields[fields.length-1] = new StructField(name, DataTypes.IntegerType, true, Metadata.empty());
        else if( calcType.equals("Double") ) fields[fields.length-1] = new StructField(name, DataTypes.DoubleType, true, Metadata.empty());
        else fields[fields.length-1] = new StructField(name, DataTypes.StringType, true, Metadata.empty());
        return new StructType(fields);
    }

    private StructType deriveSchema(String name, StructType schema) {
        StructField[] fields = new StructField[schema.size()+1];
        StructField[] sfields = schema.fields();
        IntStream.range(0, schema.size()).forEach(i -> fields[i] = sfields[i]);
        if( calcType.equals("Int") ) fields[fields.length-1] = new StructField(name, DataTypes.IntegerType, true, Metadata.empty());
        else if( calcType.equals("Double") ) fields[fields.length-1] = new StructField(name, DataTypes.DoubleType, true, Metadata.empty());
        else fields[fields.length-1] = new StructField(name, DataTypes.StringType, true, Metadata.empty());
        return new StructType(fields);
    }

    @Override
    public Row call(Row row) {
        return apply(row);
    }
}
