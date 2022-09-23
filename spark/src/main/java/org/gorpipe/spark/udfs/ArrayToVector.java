package org.gorpipe.spark.udfs;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;

public class ArrayToVector implements UDF1<double[], Vector> {
    @Override
    public Vector call(double[] d) {
        return Vectors.dense(d);
    }
}
