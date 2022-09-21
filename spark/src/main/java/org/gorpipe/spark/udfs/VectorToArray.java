package org.gorpipe.spark.udfs;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.api.java.UDF1;

public class VectorToArray implements UDF1<Vector, double[]> {
    @Override
    public double[] call(Vector v) {
        return v.toArray();
    }
}
