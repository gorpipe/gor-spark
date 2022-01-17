package org.gorpipe.spark;

import gorsat.process.GorDataType;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.gorpipe.gor.binsearch.CompressionType;
import org.gorpipe.gor.binsearch.Unzipper;
import org.gorpipe.util.collection.ByteArray;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.IntegerType;

public class GorzFlatMapFunction implements FlatMapFunction<Row, Row> {
    Unzipper unzip = new Unzipper(false);
    GorDataType gorDataType;

    public GorzFlatMapFunction(GorDataType gorDataType) {
        this.gorDataType = gorDataType;
    }

    @Override
    public Iterator<Row> call(Row row) throws Exception {
        Map<Integer, DataType> dataTypeMap = gorDataType.dataTypeMap;
        String zip = gorDataType.withStart ? row.getString(3) : row.getString(2);
        char tp = zip.charAt(0);
        final CompressionType compressionLibrary = (tp & 0x02) == 0 ? CompressionType.ZLIB : CompressionType.ZSTD;
        String zipo = zip.substring(1);
        ByteBuffer bb;
        try {
            bb = ByteBuffer.wrap(Base64.getDecoder().decode(zipo));
        } catch (Exception e) {
            bb = ByteBuffer.wrap(ByteArray.to8Bit(zipo.getBytes()));
        }
        unzip.setType(compressionLibrary);
        unzip.setInput(bb, 0, bb.capacity());
        int unzipLen = unzip.decompress(0, 1 << 17);
        ByteArrayInputStream bais = new ByteArrayInputStream(unzip.out.array(), 0, unzipLen);
        InputStreamReader isr = new InputStreamReader(bais);
        BufferedReader br = new BufferedReader(isr);
        return (gorDataType.nor ? br.lines().map(line -> {
            String[] split = line.split("\t");
            Object[] objs = new Object[split.length];
            for (int i = 0; i < split.length; i++) {
                if (dataTypeMap.containsKey(i)) {
                    if (dataTypeMap.get(i) == IntegerType)
                        objs[i] = Integer.parseInt(split[i]);
                    else objs[i] = Double.parseDouble(split[i]);
                } else objs[i] = split[i];
            }
            return RowFactory.create(objs);
        }) : br.lines().map(line -> {
            String[] split = line.split("\t");
            Object[] objs = new Object[split.length];
            objs[0] = split[0];
            objs[1] = Integer.parseInt(split[1]);
            for (int i = 2; i < split.length; i++) {
                if (dataTypeMap.containsKey(i)) {
                    if (dataTypeMap.get(i) == IntegerType)
                        objs[i] = Integer.parseInt(split[i]);
                    else objs[i] = Double.parseDouble(split[i]);
                } else objs[i] = split[i];
            }
            return RowFactory.create(objs);
        })).iterator();
    }
}