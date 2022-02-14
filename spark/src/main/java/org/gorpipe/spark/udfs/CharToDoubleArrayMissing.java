package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

public class CharToDoubleArrayMissing implements UDF1<String, double[]> {
    @Override
    public double[] call(String s) {
        double[] ret = new double[s.length()];
        for(int i = 0; i < s.length(); i++) {
            ret[i] = ((s.charAt(i)-'0')+1)%4-1;
        }
        return ret;
    }
}
