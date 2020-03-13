package org.gorpipe.spark;

import org.apache.spark.api.java.function.ReduceFunction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

public class GorHeaderInferFunction implements ReduceFunction<String>, BinaryOperator<String>, Serializable {
    Map<String,Character> convMap = new HashMap<>();

    public GorHeaderInferFunction() {
        convMap.put("IS",'S');
        convMap.put("SI",'S');
        convMap.put("DS",'S');
        convMap.put("SD",'S');
        convMap.put("ID",'D');
        convMap.put("DI",'D');
    }

    void inferOther(String[] row, String[] t1) {
        for( int i = 0; i < row.length; i++ ) {
            String colval = row[i];
            final char c = colval.charAt(colval.length() - 2);
            if (c == 'I') {
                try {
                    Integer.parseInt(t1[i]);
                } catch (Exception e) {
                    row[i] = row[i].substring(0,row[i].length()-3) + "D]";
                }
            }
            if (c == 'D') {
                try {
                    Double.parseDouble(t1[i]);
                } catch (Exception e) {
                    row[i] = row[i].substring(0,row[i].length()-3) + "S]";
                }
            }
        }
    }

    String infer(String row, String t1) {
        String[] srow = row.split("\t");
        String[] st1 = t1.split("\t");
        if( srow[0].endsWith("]") ) {
            if( st1[0].endsWith("]") ) {
                for( int i = 0; i < srow.length; i++ ) {
                    String duo = srow[i].charAt(srow[i].length()-2)+""+st1[i].charAt(st1[i].length()-2);
                    if( convMap.containsKey(duo) ) {
                        srow[i] =  srow[i].substring(0,srow[i].length()-3) + convMap.get(duo) + ']';
                    }
                }
            } else inferOther(srow, st1);
            return String.join("\t", srow);
        } else {
            if( st1[0].endsWith("]") ) {
                inferOther(st1,srow);
                return t1;
            } else {
                String[] newrow = new String[srow.length];
                for( int i = 0; i < newrow.length; i++ ) {
                    newrow[i] = srow[i]+"[I]";
                }
                inferOther(newrow, srow);
                inferOther(newrow, st1);
                return String.join("\t",newrow);
            }
        }
    }

    @Override
    public String call(String row, String t1) {
        return infer(row,t1);
    }

    @Override
    public String apply(String row, String row2) {
        return infer(row,row2);
    }
}
