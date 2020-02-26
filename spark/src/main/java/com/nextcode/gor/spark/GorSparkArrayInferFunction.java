package com.nextcode.gor.spark;

import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GorSparkArrayInferFunction implements ReduceFunction<Row> {
    static Map<String,Character> convMap = new HashMap<>();

    static {
        convMap.put("IS",'S');
        convMap.put("SI",'S');
        convMap.put("DS",'S');
        convMap.put("SD",'S');
        convMap.put("ID",'D');
        convMap.put("DI",'D');
    }

    static class InferRow extends GenericRow {
        public InferRow(Object[] objs) {
            super(objs);
        }
    }

    void checkNumericTypes(Object[] row, Row t1, int i) {
        if (row[i].equals("I")) {
            try {
                t1.getInt(i);
            } catch (Exception e) {
                row[i] = "D";
            }
        }
        if (row[i].equals("D")) {
            try {
                t1.getDouble(i);
            } catch (Exception e) {
                row[i] = "S";
            }
        }
    }

    void inferOther(Object[] row, Row t1) {
        for( int i = 0; i < row.length; i++ ) {
            if( t1.get(i).toString().length() > 16 ) {
                row[i] = "S";
            } else {
                checkNumericTypes(row, t1, i);
            }
        }
    }

    Object[] inferBoth(Row row, Row t1) {
        Object[] objs = row2Objs(row);
        inferOther(objs, row);
        inferOther(objs, t1);
        return objs;
    }

    Object[] row2Objs(Row row) {
        Object[] objs = new Object[row.length()];
        Arrays.fill(objs, "I");
        return objs;
    }

    public Row infer(Row row, Row t1) {
        if( row instanceof InferRow ) {
            if( t1 instanceof InferRow ) {
                for( int i = 0; i < row.length(); i++ ) {
                    String duo = row.getString(i)+t1.getString(i);
                    if( convMap.containsKey(duo) ) {
                        Object[] objs = row2Objs(row);
                        inferOther(objs, t1);
                        objs[i] = convMap.get(duo);
                        row = new InferRow(objs);
                    }
                }
            } else {
                Object[] objs = row2Objs(row);
                inferOther(objs, t1);
                row = new InferRow(objs);
            }
            return row;
        } else {
            if( t1 instanceof InferRow ) {
                Object[] objs = row2Objs(t1);
                inferOther(objs,row);
                return new InferRow(objs);
            } else {
                return new InferRow(inferBoth(row, t1));
            }
        }
    }

    @Override
    public Row call(Row row, Row t1) {
        return infer(row,t1);
    }
}

