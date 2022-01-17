package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF3;

import java.util.stream.IntStream;

public class XorAndBitCount implements UDF3<long[], long[], long[], Long> {
    @Override
    public Long call(long[] l1, long[] l2, long[] and) throws Exception {
        int len = Math.min(l1.length,l2.length);
        return IntStream.range(0,len).parallel().mapToLong(i -> Long.bitCount((l1[i]^l2[i])&and[i])).sum();
    }
}
