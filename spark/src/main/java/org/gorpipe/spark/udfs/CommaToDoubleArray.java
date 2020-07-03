package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

public class CommaToDoubleArray implements UDF1<String, double[]> {
    @Override
    public double[] call(String s) {
        String[] spl = s.split(",");
        double[] ret = new double[spl.length];
        for(int i = 0; i < spl.length; i++) {
            ret[i] = Double.parseDouble(spl[i]);
        }
        return ret;
    }
}
