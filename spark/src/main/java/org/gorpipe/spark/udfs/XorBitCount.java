package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF2;

import java.util.stream.IntStream;

public class XorBitCount implements UDF2<long[], long[], Long> {
    @Override
    public Long call(long[] l1, long[] l2) throws Exception {
        int len = Math.min(l1.length,l2.length);
        return IntStream.range(0,len).parallel().mapToLong(i -> Long.bitCount(l1[i]^l2[i])).sum();
    }
}
