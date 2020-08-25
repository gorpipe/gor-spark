package org.gorpipe.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.Writer;

public class SparkGorRow extends GorSparkRowBase implements Serializable {
    public org.gorpipe.gor.model.Row row;
    StructType schema;

    public SparkGorRow(StructType schema) {
        this.schema = schema;
    }

    public SparkGorRow(org.gorpipe.gor.model.Row row, StructType schema) {
        this(schema);
        this.row = row;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public String toString() {
        return row.toString();
    }

    @Override
    public int size() {
        return row.numCols();
    }

    @Override
    public String toColString() {
        return row.toColString();
    }

    @Override
    public int colAsInt(int colNum) {
        return row.colAsInt(colNum);
    }

    @Override
    public double colAsDouble(int colNum) {
        return row.colAsDouble(colNum);
    }

    @Override
    public Long colAsLong(int colNum) {
        return row.colAsLong(colNum);
    }

    @Override
    public CharSequence colAsString(int colNum) {
        return row.colAsString(colNum);
    }

    @Override
    public String otherCols() {
        return row.otherCols();
    }

    @Override
    public CharSequence colsSlice(int startCol, int stopCol) {
        return row.colsSlice(startCol, stopCol);
    }

    @Override
    public CharSequence getAllCols() {
        return row.getAllCols();
    }

    @Override
    public int numCols() {
        return row.numCols();
    }

    @Override
    public int length() {
        return row.numCols();
    }

    @Override
    public String selectedColumns(int[] columnIndices) {
        return row.selectedColumns(columnIndices);
    }

    @Override
    public int otherColsLength() {
        return row.otherColsLength();
    }

    @Override
    public void addSingleColumnToRow(String rowString) {
        row.addSingleColumnToRow(rowString);
    }

    @Override
    public org.gorpipe.gor.model.Row slicedRow(int startCol, int stopCol) {
        return row.slicedRow(startCol, stopCol);
    }

    @Override
    public org.gorpipe.gor.model.Row rowWithSelectedColumns(int[] columnIndices) {
        return row.rowWithSelectedColumns(columnIndices);
    }

    @Override
    public int sa(int i) {
        return row.sa(i);
    }

    @Override
    public void resize(int newsize) {
        row.resize(newsize);
    }

    @Override
    public void setColumn(int i, String val) {
        row.setColumn(i,val);
    }

    @Override
    public void addColumns(int num) {
        row.addColumns(num);
    }

    @Override
    public void removeColumn(int n) {
        row.removeColumn(n);
    }

    @Override
    public char peekAtColumn(int n) {
        return row.peekAtColumn(n);
    }

    @Override
    public void writeRow(Writer outputStream) throws IOException {
        row.writeRow(outputStream);
    }

    @Override
    public void writeRowToStream(OutputStream outputStream) throws IOException {
        row.writeRowToStream(outputStream);
    }

    @Override
    public Object apply(int i) {
        return row.colAsString(i);
    }

    @Override
    public Object get(int i) {
        DataType dt = schema.fields()[i].dataType();
        if(dt == DataTypes.IntegerType) {
            try {
                return row.intValue(i);
            } catch(Exception e) {
                return -1;
            }
        } else if(dt == DataTypes.FloatType) {
            try {
                return (float) row.doubleValue(i);
            } catch(Exception e) {
                return Float.NaN;
            }
        } else if(dt == DataTypes.DoubleType) {
            try {
                return row.doubleValue(i);
            } catch(Exception e) {
                return Double.NaN;
            }
        } else if(dt == DataTypes.LongType) {
            return row.colAsLong(i);
        } else {
            return row.stringValue(i);
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return false;
    }

    @Override
    public String getString(int i) {
        return row.colAsString(i).toString();
    }

    @Override
    public Row copy() {
        return new SparkGorRow(row.copyRow(), schema);
    }

    @Override
    public boolean getBoolean(int i) {
        return Boolean.parseBoolean(row.colAsString(i).toString());
    }

    @Override
    public int getInt(int i) {
        return row.colAsInt(i);
    }

    @Override
    public long getLong(int i) {
        return row.colAsLong(i);
    }

    @Override
    public float getFloat(int i) {
        try {
            return (float) row.colAsDouble(i);
        } catch(Exception e) {
            return Float.NaN;
        }
    }

    @Override
    public String stringValue(int col) {
        return row.colAsString(col).toString();
    }

    @Override
    public int intValue(int col) {
        return row.colAsInt(col);
    }

    @Override
    public long longValue(int col) {
        return row.colAsLong(col);
    }

    @Override
    public double doubleValue(int col) {
        try {
            return row.colAsDouble(col);
        } catch(Exception e) {
            return Double.NaN;
        }
    }
}
