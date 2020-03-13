package org.gorpipe.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.gorpipe.model.genome.files.binsearch.CompressionType;
import org.gorpipe.model.genome.files.binsearch.Unzipper;
import org.gorpipe.util.collection.ByteArray;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
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
        Unzipper unzip = new Unzipper();
        unzip.setType(compressionLibrary);
        unzip.setRawInput(bb,0,bb.length);
        if(buffer == null) buffer = new byte[1<<17];
        int unzipLen = unzip.decompress(buffer, 0, buffer.length);
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer, 0, unzipLen);
        InputStreamReader isr = new InputStreamReader(bais);
        BufferedReader br = new BufferedReader(isr);
        return br.lines().iterator();
    }
}