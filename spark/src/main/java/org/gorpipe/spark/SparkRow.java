package org.gorpipe.spark;


import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.Writer;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.model.Row;
import org.gorpipe.model.gor.RowObj;

public class SparkRow extends GorSparkRowBase implements Serializable {
    public org.apache.spark.sql.Row row;

    public SparkRow(org.apache.spark.sql.Row sparkrow) {
        row = sparkrow;
        init();
    }

    public void init() {
        chr = "chrN";
        pos = 0;
    }

    @Override
    public Object apply(int i) {
        return row.apply(i);
    }

    @Override
    public Object get(int i) {
        return row.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return row.isNullAt(i);
    }

    @Override
    public String getString(int i) {
        return row.getString(i);
    }

    @Override
    public org.apache.spark.sql.Row copy() {
        return row.copy();
    }

    @Override
    public boolean getBoolean(int i) {
        return row.getBoolean(i);
    }

    @Override
    public int getInt(int i) {
        return row.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return row.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return row.getFloat(i);
    }

    public StructType schema() {
        return row.schema() ;
    }

    @Override
    public String stringValue(int col) {
        return row.getString(col);
    }

    @Override
    public int intValue(int col) {
        return row.getInt(col);
    }

    @Override
    public long longValue(int col) {
        if( schema().fields()[col].dataType() == DataTypes.IntegerType) {
            return row.getInt(col);
        }
        return row.getLong(col);
    }

    @Override
    public double doubleValue(int col) {
        return row.getDouble(col);
    }

    @Override
    public String toColString() {
        return row.mkString("(", ") (", ")");
    }

    @Override
    public int colAsInt(int colNum) {
        return row.getInt(colNum);
    }

    @Override
    public double colAsDouble(int colNum) {
        return row.getDouble(colNum);
    }

    @Override
    public long colAsLong(int colNum) {
        return row.getLong(colNum);
    }

    @Override
    public String colAsString(int colNum) {
        return row.get(colNum).toString();//row.getString(colNum);
    }

    @Override
    public String otherCols() {
        return row.mkString("\t");
    }

    @Override
    public CharSequence colsSlice(int startCol, int stopCol) {
        return IntStream.range(startCol, stopCol).mapToObj(i -> row.get(i).toString()).collect(Collectors.joining("\t"));
    }

    @Override
    public CharSequence getAllCols() {
        return row.mkString("\t");
    }

    @Override
    public String toString() {
        return getAllCols().toString();
    }

    @Override
    public int numCols() {
        return row.length();
    }

    @Override
    public String selectedColumns(int[] columnIndices) {
        return Arrays.stream(columnIndices).mapToObj(i -> row.getString(i)).collect(Collectors.joining("\t"));
    }

    @Override
    public int otherColsLength() {
        return otherCols().length();
    }

    @Override
    public void addSingleColumnToRow(String rowString) {
        row = RowFactory.create(Stream.concat(IntStream.range(0, row.length()).mapToObj(i -> row.get(i)),Stream.of(rowString)).toArray(Object[]::new));
    }

    @Override
    public Row slicedRow(int startCol, int stopCol) {
        return RowObj.apply(colsSlice(startCol,stopCol));
    }

    @Override
    public Row rowWithSelectedColumns(int[] columnIndices) {
        return RowObj.apply(Arrays.stream(columnIndices).mapToObj(i -> row.get(i).toString()).collect(Collectors.joining("\t")));
    }

    @Override
    public int sa(int i) {
        return IntStream.rangeClosed(0,i).map(k -> colAsString(k).length()).sum()+i;
    }

    @Override
    public void resize(int newsize) {
        row = RowFactory.create(IntStream.range(0, newsize).mapToObj(i -> row.get(i)).toArray(Object[]::new));
    }

    @Override
    public void setColumn(int k, String val) {
        Object[] objs = new Object[Math.max(row.length(),k+3)];
        for(int i = 0; i < row.length(); i++ ) {
            objs[i] = row.get(i);
        }
        objs[k+2] = val;
        row = RowFactory.create(objs);
    }

    @Override
    public void addColumns(int num) {}

    @Override
    public void removeColumn(int n) {
        Stream<Object> rowstream = Stream.concat(IntStream.range(0, n).mapToObj(i -> row.get(i)), IntStream.range(n+1, row.length()).mapToObj(i -> row.get(i)));
        row = RowFactory.create(rowstream.toArray(Object[]::new));
    }

    @Override
    public char peekAtColumn(int n) {
        return 0;
    }

    @Override
    public void writeRow(Writer outputStream) {}

    @Override
    public void writeRowToStream(OutputStream outputStream) throws IOException {
        int i = 0;
        for( ; i < row.length()-1; i++ ) {
            outputStream.write(row.get(i).toString().getBytes());
            outputStream.write('\t');
        }
        outputStream.write(row.get(i).toString().getBytes());
    }

    @Override
    public void writeNorRowToStream(OutputStream outputStream) throws IOException {
        writeRowToStream(outputStream);
    }

    @Override
    public int length() {
        return row.length();
    }
}