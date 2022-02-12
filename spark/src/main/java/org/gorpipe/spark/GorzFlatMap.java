package org.gorpipe.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.gorpipe.gor.binsearch.CompressionType;
import org.gorpipe.gor.binsearch.Unzipper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Iterator;

public class GorzFlatMap implements FlatMapFunction<Row, String> {
    byte[] buffer;

    @Override
    public Iterator<String> call(Row row) throws Exception {
        String zip = row.getString(2);
        char tp = zip.charAt(0);
        final CompressionType compressionLibrary = (tp & 0x02) == 0 ? CompressionType.ZLIB : CompressionType.ZSTD;
        byte[] bb = Base64.getDecoder().decode(zip);
        var byteBuffer = ByteBuffer.wrap(bb);
        Unzipper unzip = new Unzipper();
        unzip.setType(compressionLibrary);
        unzip.setInput(byteBuffer,0,bb.length);
        int unzipLen = unzip.decompress(0);
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer, 0, unzipLen);
        InputStreamReader isr = new InputStreamReader(bais);
        BufferedReader br = new BufferedReader(isr);
        return br.lines().iterator();
    }
}