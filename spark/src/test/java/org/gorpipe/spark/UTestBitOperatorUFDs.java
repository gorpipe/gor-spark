package org.gorpipe.spark;

import org.gorpipe.spark.udfs.XorBitCount;
import org.junit.Assert;
import org.junit.Test;

public class UTestBitOperatorUFDs {
    @Test
    public void testXorBitCount() throws Exception {
        XorBitCount xorBitCount = new XorBitCount();
        var l0 = Long.parseLong("-2");
        var l1 = Long.parseLong("010101010101010101010101010101010101010101010101010101010101010",2);
        var l2 = Long.parseLong("101010101010101010101010101010101010101010101010101010101010101",2);
        var ll0 = new long[] {-1};
        var ll1 = new long[] {l1};
        var ll2 = new long[] {l2};

        var l = Long.parseLong("-100",2);
        System.err.println(l);
        System.err.println(Long.toBinaryString(l));
        System.err.println(Long.toBinaryString(l1));
        System.err.println(Long.toBinaryString(l2));
        //Assert.assertEquals(64L,xorBitCount.call(ll1,ll2).longValue());
        //Assert.assertEquals(0L,xorBitCount.call(ll1,ll1).longValue());
    }
}
