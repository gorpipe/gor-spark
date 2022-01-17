package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

import java.util.Arrays;

public class BitCount  implements UDF1<long[], Long> {

    @Override
    public Long call(long[] longs) throws Exception {
        return Arrays.stream(longs).parallel().map(Long::bitCount).sum();
    }
}
