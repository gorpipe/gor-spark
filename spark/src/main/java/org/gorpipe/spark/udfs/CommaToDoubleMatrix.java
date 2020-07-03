package org.gorpipe.spark.udfs;

import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.sql.api.java.UDF1;

public class CommaToDoubleMatrix implements UDF1<String, DenseMatrix> {

    @Override
    public DenseMatrix call(String s) {
        String[] spl = s.split(";");
        String[] subspl = spl[0].split(",");
        double[] ret = new double[spl.length*subspl.length];
        for(int i = 0; i < spl.length; i++) {
            subspl = spl[i].split(",");
            for(int k = 0; k < subspl.length; k++) {
                ret[i*spl.length+k] = Double.parseDouble(subspl[k]);
            }
        }
        DenseMatrix dm = new DenseMatrix(subspl.length, spl.length, ret, false);
        return dm;
    }
}
