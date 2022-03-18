package org.gorpipe.spark.udfs;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;

public class ListToVector implements UDF1<String, Vector> {
    @Override
    public Vector call(String s) {
        String[] spl = s.split(",");
        double[] ret = new double[spl.length];
        for(int i = 0; i < spl.length; i++) {
            ret[i] = Double.parseDouble(spl[i]);
        }
        return Vectors.dense(ret);
    }
}
