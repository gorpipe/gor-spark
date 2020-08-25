package org.gorpipe.spark;

import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.model.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

public abstract class GorSparkRowBase extends Row implements org.apache.spark.sql.Row {
    @Override
    public abstract Object apply(int i);

    @Override
    public abstract Object get(int i);

    @Override
    public abstract boolean isNullAt(int i);

    @Override
    public abstract String getString(int i);

    @Override
    public abstract org.apache.spark.sql.Row copy();

    @Override
    public abstract boolean getBoolean(int i);

    @Override
    public abstract int getInt(int i);

    @Override
    public abstract long getLong(int i);

    @Override
    public abstract float getFloat(int i);

    public abstract StructType schema();

    @Override
    public abstract String stringValue(int col);

    @Override
    public abstract int intValue(int col);

    @Override
    public abstract long longValue(int col);

    @Override
    public abstract double doubleValue(int col);

    @Override
    public abstract String toColString();

    @Override
    public abstract int colAsInt(int colNum);

    @Override
    public abstract double colAsDouble(int colNum);

    @Override
    public abstract Long colAsLong(int colNum);

    @Override
    public abstract CharSequence colAsString(int colNum);

    @Override
    public abstract String otherCols();

    @Override
    public abstract CharSequence colsSlice(int startCol, int stopCol);

    @Override
    public abstract CharSequence getAllCols();

    @Override
    public abstract String toString();

    @Override
    public abstract int numCols();

    @Override
    public abstract String selectedColumns(int[] columnIndices);

    @Override
    public abstract int otherColsLength();

    @Override
    public abstract void addSingleColumnToRow(String rowString);

    @Override
    public abstract Row slicedRow(int startCol, int stopCol);

    @Override
    public abstract Row rowWithSelectedColumns(int[] columnIndices);

    @Override
    public abstract int sa(int i);

    @Override
    public abstract void resize(int newsize);

    @Override
    public abstract void setColumn(int k, String val);

    @Override
    public abstract void addColumns(int num);

    @Override
    public abstract void removeColumn(int n);

    @Override
    public abstract char peekAtColumn(int n);

    @Override
    public abstract void writeRow(Writer outputStream) throws IOException;

    @Override
    public abstract void writeRowToStream(OutputStream outputStream) throws IOException;

    @Override
    public abstract int length();
}
