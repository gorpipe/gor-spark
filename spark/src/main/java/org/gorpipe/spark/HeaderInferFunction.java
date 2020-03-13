package org.gorpipe.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.gorpipe.model.genome.files.gor.Row;
import org.gorpipe.model.gor.RowObj;

public class HeaderInferFunction implements MapPartitionsFunction<String, String>, Serializable {
    String cmd;
    String header;

    public HeaderInferFunction(String cmd, String header) {
        this.cmd = cmd;
        this.header = header;
    }

    @Override
    public Iterator<String> call(Iterator<String> iterator) throws Exception {
        String[] split = cmd.split(" ");
        ProcessBuilder pb = new ProcessBuilder(split);
        Process p = pb.start();
        OutputStream os = p.getOutputStream();
        InputStream err = p.getErrorStream();
        Thread t = new Thread(() -> {
            try {
                os.write(header.getBytes());
                os.write('\n');
                while( iterator.hasNext() ) {
                    os.write(iterator.next().getBytes());
                    os.write('\n');
                }
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t.start();
        Thread t2 = new Thread(() -> {
            try {
                int r = err.read();
                while( r != -1 ) {
                    System.err.print((char)r);
                    r = err.read();
                }
                System.err.println();
                err.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t2.start();

        InputStreamReader isr = new InputStreamReader(p.getInputStream());
        BufferedReader br = new BufferedReader(isr);
        String header = br.readLine();
        GorSparkRowInferFunction gif = new GorSparkRowInferFunction();
        Optional<Row> or = br.lines().map(RowObj::apply).reduce(gif);
        if( or.isPresent() ) {
            Row r = or.get();
            String[] hsplit = header.split("\t");
            StringBuilder sb = new StringBuilder();
            sb.append(hsplit[0]).append('[').append(r.colAsString(0)).append(']');
            for( int i = 1; i < hsplit.length; i++ ) {
                sb.append('\t').append(hsplit[i]).append('[').append(r.colAsString(i)).append(']');
            }
            return Collections.singletonList(sb.toString()).iterator();
        }
        return Collections.singletonList(header).iterator();
    }
}
