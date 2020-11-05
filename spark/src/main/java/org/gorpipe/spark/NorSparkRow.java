package org.gorpipe.spark;

import org.apache.spark.sql.RowFactory;

public class NorSparkRow extends SparkRow {
    public NorSparkRow(org.apache.spark.sql.Row sparkrow) {
        super(sparkrow);
    }

    @Override
    public int numCols() {
        return row.length()+2;
    }

    @Override
    public String colAsString(int colNum) {
        if(colNum==0) return chr;
        else if(colNum==1) return Integer.toString(pos);
        return row.get(colNum-2).toString();
    }

    @Override
    public String stringValue(int col) {
        if(col==0) return chr;
        else if(col==1) return Integer.toString(pos);
        return row.get(col-2).toString();
    }

    @Override
    public void setColumn(int k, String val) {
        Object[] objs = new Object[Math.max(row.length(),k+1)];
        for(int i = 0; i < row.length(); i++ ) {
            objs[i] = row.get(i);
        }
        objs[k] = val;
        row = RowFactory.create(objs);
    }
}
