package org.gorpipe.spark.udfs;

import org.apache.spark.sql.api.java.UDF2;

public class StringToBitArray implements UDF2<String, Character, long[]> {
    static final int SIZEOF = 64;

    @Override
    public long[] call(String s, Character cc) {
        long[] ret = new long[(s.length()+SIZEOF-1)/SIZEOF];
        int len = SIZEOF*(s.length()/SIZEOF);
        int i;
        long l;
        for(i = 0; i < len; i+=SIZEOF) {
            int k = i/SIZEOF;
            l = 0;
            for(int y = 0; y < SIZEOF; y++) {
                char c = s.charAt(i+y);
                l |= c == cc ? 1L << y : 0;
            }
            ret[k] = l;
        }
        for(i = 0; i < s.length(); i+=SIZEOF) {
            int k = i / SIZEOF;
            l = 0;
            for (int y = 0; y < s.length()-len; y++) {
                char c = s.charAt(i + y);
                l |= c == cc ? 1L << y : 0;
            }
            ret[k] = l;
        }
        return ret;
    }
}
