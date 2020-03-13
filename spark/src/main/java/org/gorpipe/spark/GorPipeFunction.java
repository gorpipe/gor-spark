package org.gorpipe.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.gorpipe.model.genome.files.gor.Row;
import org.gorpipe.model.gor.RowObj;

public class GorPipeFunction implements MapPartitionsFunction<Row, Row>, Serializable {
    String cmd;
    String header;

    public GorPipeFunction(String cmd, String header) {
        this.cmd = cmd;
        this.header = header;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
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
                    os.write(iterator.next().toString().getBytes());
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
        return br.lines().skip(1).map(RowObj::apply).iterator();
    }
}
