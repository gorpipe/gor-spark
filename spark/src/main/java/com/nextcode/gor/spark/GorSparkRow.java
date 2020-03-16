package com.nextcode.gor.spark;

import org.apache.spark.sql.RowFactory;

public class GorSparkRow extends SparkRow {
    public GorSparkRow(org.apache.spark.sql.Row sparkrow) {
        super(sparkrow);
    }

    @Override
    public void init() {
        chr = row.getString(0);
        if( chr != null ) {
            pos = row.getInt(1);
        } else {
            chr = "chrN";
            pos = -1;

            Object[] objs = new Object[] {chr, pos};
            row = RowFactory.create(objs);
        }
    }

    @Override
    public String otherCols() {
        StringBuilder sb = new StringBuilder();
        sb.append(row.get(2));
        for( int i = 3; i < row.length(); i++ ) {
            sb.append("\t");
            sb.append(row.get(i));
        }
        return sb.toString();
    }

    @Override
    public String stringValue(int col) {
        //if( col == 0 ) return chr;
        //else if( col == 1 ) return Integer.toString(pos);
        return row.get(col).toString();
    }
}
