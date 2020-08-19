package org.gorpipe.spark;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class GorpWriter implements MapPartitionsFunction<Row, Row> {
    String path;

    public GorpWriter(String path) {
        this.path = path;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) {
        return new Iterator<Row>() {
            Row first;
            Row last;

            @Override
            public boolean hasNext() {
                boolean ret = input.hasNext();
                if(!ret) {
                    try {
                        Path gorp = Paths.get(path);
                        Files.writeString(gorp,first.getString(0)+"\t"+first.getInt(1)+"\t"+last.getString(0)+"\t"+last.getInt(1)+"\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return ret;
            }

            @Override
            public Row next() {
                last = input.next();
                if(first==null) first = last;
                return last;
            }
        };
    }
}
