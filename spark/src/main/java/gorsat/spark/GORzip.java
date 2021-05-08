package gorsat.spark;

import gorsat.Commands.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gorpipe.exceptions.GorResourceException;
import org.gorpipe.gor.binsearch.GorIndexType;
import org.gorpipe.gor.binsearch.GorZipLexOutputStream;
import org.gorpipe.gor.model.GorMeta;
import org.gorpipe.gor.model.Row;

import java.io.IOException;

public class GORzip extends Output {
    GorZipLexOutputStream out;
    Path path;
    FileSystem fs;
    String header;
    boolean skipHeader;
    String cardCol;

    public GORzip(Path path, String header, boolean skipHeader, boolean colcompress, GorIndexType idx, int compressionLevel, String cardCol) throws IOException {
        Configuration conf = new Configuration();
        fs = path.getFileSystem(conf);
        FSDataOutputStream os = fs.create(path);
        this.out = new GorZipLexOutputStream(os, colcompress, true, null, null, idx, compressionLevel);
        this.path = path;

        this.header = header;
        this.skipHeader = skipHeader;
        this.cardCol = cardCol;
    }

    @Override
    public void setup() {
        GorMeta meta = getMeta();
        if (cardCol != null) meta.initCardCol(cardCol, header);
        if (header != null & !skipHeader) {
            try {
                out.setHeader(header);
            } catch (IOException e) {
                throw new GorResourceException("", "", e);
            }
        }
    }

    @Override
    public void process(Row r) {
        getMeta().updateRange(r);
        try {
            out.write(r);
        } catch (IOException e) {
            throw new GorResourceException("Unable to write gorz", path.toString(), e);
        }
    }

    @Override
    public void finish() {
        Path metapath = new Path(path.getParent(),path.getName() + ".meta");
        try {
            out.close();
            getMeta().setMd5(out.getMd5());
            if(getMeta().linesWritten()) {
                FSDataOutputStream metaout = fs.create(metapath);
                metaout.write(getMeta().toString().getBytes());
                metaout.close();
            }
        } catch (IOException e) {
            throw new GorResourceException("Unable to write gorz meta", path.toString(), e);
        }
    }
}
