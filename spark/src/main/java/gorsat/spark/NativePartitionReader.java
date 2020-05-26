package gorsat.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.LongType;

public class NativePartitionReader implements PartitionReader<InternalRow> {
    BufferedReader br;
    String currentLine;
    GenericInternalRow gir;
    StructField[] fields;
    GorRangeInputPartition p;

    public NativePartitionReader(StructField[] fields, GorRangeInputPartition gorRangeInputPartition) {
        this.fields = fields;
        gir = new GenericInternalRow(new Object[fields.length]);
        p = gorRangeInputPartition;
    }

    @Override
    public boolean next() {
        try {
            if(br==null) {
                ProcessBuilder processBuilder = new ProcessBuilder("cgor", "-p", p.chr, p.path);
                Process process = processBuilder.start();
                InputStream is = process.getInputStream();
                InputStreamReader isr = new InputStreamReader(is);
                br = new BufferedReader(isr);
                br.readLine();
            }
            currentLine = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return currentLine != null;
    }

    @Override
    public InternalRow get() {
        int start = 0;
        int i = 0;
        for(; i < fields.length-1; i++) {
            int last = currentLine.indexOf('\t', start+1);
            String str = currentLine.substring(start, last);
            StructField sf = fields[i];
            if(sf.dataType() == StringType) {
                gir.update(i, UTF8String.fromString(str));
            } else if(sf.dataType() == IntegerType) {
                gir.update(i, Integer.parseInt(str));
            } else if(sf.dataType() == DoubleType) {
                gir.update(i, Double.parseDouble(str));
            } else if(sf.dataType() == LongType) {
                gir.update(i, Long.parseLong(str));
            }
            start = last+1;
        }

        int last = currentLine.length();
        String str = currentLine.substring(start, last);
        StructField sf = fields[i];
        if(sf.dataType() == StringType) {
            gir.update(i, UTF8String.fromString(str));
        } else if(sf.dataType() == IntegerType) {
            gir.update(i, Integer.parseInt(str));
        } else if(sf.dataType() == DoubleType) {
            gir.update(i, Double.parseDouble(str));
        } else if(sf.dataType() == LongType) {
            gir.update(i, Long.parseLong(str));
        }

        return gir;
    }

    @Override
    public void close() throws IOException {
        br.close();
    }
}
