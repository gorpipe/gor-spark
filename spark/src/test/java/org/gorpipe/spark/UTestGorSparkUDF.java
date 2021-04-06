package org.gorpipe.spark;

import org.gorpipe.spark.udfs.CharToDoubleArrayParallel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class UTestGorSparkUDF {

    @Test
    public void testCharsToDoubleArrayParallel() {
        var ctda = new CharToDoubleArrayParallel();
        var zstr = "0000000000";
        var da = ctda.call(zstr);
        var zr = new double[zstr.length()];
        var cmp = Arrays.compare(da, zr);
        Assert.assertEquals("Chars to double array conversion not returning correct results",0,cmp);
    }
}
