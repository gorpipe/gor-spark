package org.gorpipe.spark;

import org.apache.spark.api.java.function.MapPartitionsFunction;

import java.io.*;
import java.util.Iterator;

public class PipeFunction implements MapPartitionsFunction<String, String>, Serializable {
    String cmd;
    String header;

    public PipeFunction(String cmd, String header) {
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
                throw new RuntimeException("Unable to write to procss",e);
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
        return br.lines().skip(1).iterator();
    }
}
