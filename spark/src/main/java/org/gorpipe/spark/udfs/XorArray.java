package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF2;

import java.util.stream.IntStream;

public class XorArray implements UDF2<long[], long[], long[]> {

    @Override
    public long[] call(long[] l1, long[] l2) throws Exception {
        var ll = new long[Math.min(l1.length,l2.length)];
        IntStream.range(0, ll.length).parallel().forEach(i -> ll[i] = l1[i]^l2[i]);
        return ll;
    }
}
