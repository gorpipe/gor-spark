package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

import java.util.stream.IntStream;

public class CharToDoubleArrayParallel implements UDF1<String, double[]> {
    @Override
    public double[] call(String s) {
        double[] ret = new double[s.length()];
        IntStream.range(0,ret.length).parallel().forEach(i -> {
            ret[i] = s.charAt(i)-'0';
        });
        return ret;
    }
}
