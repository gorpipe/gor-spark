package com.nextcode.gor.spark;

public class LongGorSparkRow extends GorSparkRow {
    public LongGorSparkRow(org.apache.spark.sql.Row sparkrow) {
        super(sparkrow);
    }

    @Override
    public void init() {
        chr = row.getString(0);
        pos = (int)row.getLong(1);
    }
}
