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
    Unzipper unzip = new Unzipper(false);

    @Override
    public Iterator<String> call(Row row) throws Exception {
        String zip = row.getString(2);
        char tp = zip.charAt(0);
        final CompressionType compressionLibrary = (tp & 0x02) == 0 ? CompressionType.ZLIB : CompressionType.ZSTD;
        ByteBuffer bb = ByteBuffer.wrap(Base64.getDecoder().decode(zip));
        unzip.setType(compressionLibrary);
        unzip.setInput(bb,0,bb.capacity());
        int unzipLen = unzip.decompress(0, unzip.out.capacity());
        ByteArrayInputStream bais = new ByteArrayInputStream(unzip.out.array(), 0, unzipLen);
        InputStreamReader isr = new InputStreamReader(bais);
        BufferedReader br = new BufferedReader(isr);
        return br.lines().iterator();
    }
}