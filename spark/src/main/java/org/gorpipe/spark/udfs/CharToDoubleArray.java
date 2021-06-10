package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

public class CharToDoubleArray implements UDF1<String, double[]> {
    //long l = 0x3030303030303030L;
    //byte b = '0';

    @Override
    public double[] call(String s) {
        double[] ret = new double[s.length()];
        for(int i = 0; i < s.length(); i++) {
            ret[i] = s.charAt(i)-'0';
        }
        return ret;
    }

    /*public double[] call1(String s) {
        double[] ret = new double[s.length()];
        byte[] bb = s.getBytes();
        for(int i = 0; i < bb.length; i++) {
            ret[i] = bb[i]-b;
        }
        return ret;
    }

    public double[] call2(String s) {
        double[] ret = new double[s.length()];
        byte[] bb = s.getBytes();
        var bbb = ByteBuffer.wrap(bb);
        //var lb = bbb.asLongBuffer();
        for(int i = 0; i < bb.length-7; i+=8) {
            var lv = bbb.getLong(i);
            bbb.putLong(i, lv-l);
            for(int k = i; k < i+8; k++) {
                ret[k] = bb[k];
            }
        }
        return ret;
    }
    
    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        IntStream.range(0,500000).map(i -> i%4).forEach(sb::append);
        String val = sb.toString();
        CharToDoubleArray cda = new CharToDoubleArray();

        long tl = System.currentTimeMillis();
        for(int i = 0; i < 2000; i++) {
            cda.call(val);
        }
        long te = System.currentTimeMillis();
        System.err.println((te-tl)/1000.0);

        tl = System.currentTimeMillis();
        for(int i = 0; i < 2000; i++) {
            cda.call1(val);
        }
        te = System.currentTimeMillis();
        System.err.println((te-tl)/1000.0);

        tl = System.currentTimeMillis();
        for(int i = 0; i < 2000; i++) {
            cda.call2(val);
        }
        te = System.currentTimeMillis();
        System.err.println((te-tl)/1000.0);
    }*/
}
