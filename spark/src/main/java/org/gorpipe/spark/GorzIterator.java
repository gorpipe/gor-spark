package org.gorpipe.spark;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.gorpipe.gor.binsearch.GorZipLexOutputStream;

import java.io.*;
import java.util.Iterator;

public class GorzIterator implements MapPartitionsFunction<String, String> {
    String header;

    public GorzIterator() {
        this(null);
    }

    public GorzIterator(String header) {
        this.header = header;
    }

    @Override
    public Iterator<String> call(Iterator<String> iterator) throws Exception {
        PipedInputStream pip = new PipedInputStream();
        PipedOutputStream pos = new PipedOutputStream(pip);
        GorZipLexOutputStream gzlos = new GorZipLexOutputStream(pos, false, null, true);
        if( header != null ) gzlos.setHeader(header);
        Thread t = new Thread(() -> {
            try {
                while( iterator.hasNext() ) {
                    gzlos.write( iterator.next() );
                }
                gzlos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t.start();

        InputStreamReader isr = new InputStreamReader(pip);
        BufferedReader br = new BufferedReader(isr);
        return br.lines().iterator();
    }
}
