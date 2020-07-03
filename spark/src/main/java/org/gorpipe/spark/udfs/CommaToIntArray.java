package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

public class CommaToIntArray implements UDF1<String, int[]> {
    @Override
    public int[] call(String s) {
        String[] spl = s.split(",");
        int[] ret = new int[spl.length];
        for(int i = 0; i < spl.length; i++) {
            ret[i] = Integer.parseInt(spl[i]);
        }
        return ret;
    }
}
