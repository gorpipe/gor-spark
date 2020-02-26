package com.nextcode.gor.spark;

import org.apache.spark.Partition;

import java.io.Serializable;

public class GorPartition implements Partition, Serializable {
    private int p;

    public GorPartition(int i) {
        p = i;
    }

    @Override
    public int index() {
        return p;
    }
}
