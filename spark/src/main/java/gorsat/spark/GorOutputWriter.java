package gorsat.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.model.genome.files.binsearch.GorIndexType;
import org.gorpipe.model.genome.files.binsearch.GorZipLexOutputStream;
import org.gorpipe.spark.GorSparkRow;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.Deflater;

public class GorOutputWriter extends OutputWriter {
    GorZipLexOutputStream of;
    ExpressionEncoder<Row> encoder;
    Path path;
    String startChrom = null;
    int startPos;
    String stopChrom = null;
    int stopPos;
    String originalPath;

    public GorOutputWriter(String uristr, StructType schema, String originalPath) throws IOException {
        this.originalPath = originalPath;
        List<Attribute> lattr = JavaConverters.asJavaCollection(schema.toAttributes()).stream().map(Attribute::toAttribute).collect(Collectors.toList());
        Seq sattr = JavaConverters.asScalaBuffer(lattr).toSeq();
        encoder = RowEncoder.apply(schema).resolveAndBind(sattr, SimpleAnalyzer$.MODULE$);
        String header = String.join("\t", schema.fieldNames());
        URI uri = URI.create(uristr);
        path = Paths.get(uri);
        path.getParent().toFile().mkdirs();
        OutputStream os = Files.newOutputStream(path);
        of = new GorZipLexOutputStream(os, false, null, null, GorIndexType.NONE, Deflater.BEST_SPEED); //new OutFile(path.toString(), header, false, false);
        of.setHeader(header);
    }

    @Override
    public void write(InternalRow row) {
        try {
            Row grow = encoder.fromRow(row);
            org.gorpipe.model.genome.files.gor.Row sparkrow = new GorSparkRow(grow);

            if( startChrom == null ) {
                startChrom = sparkrow.chr;
                startPos = sparkrow.pos;
            }

            stopChrom = sparkrow.chr;
            stopPos = sparkrow.pos;

            of.write(sparkrow);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        Writer w = null;
        try {
            of.close();

            Path origPath = Paths.get(originalPath);
            w = Files.newBufferedWriter(origPath.getParent().resolve(origPath.getFileName().toString()+".gord"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            w.write(origPath.resolve(path.getFileName()) + "\t1\t" + startChrom + "\t" + startPos + "\t" + stopChrom + "\t" + stopPos + '\n');
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if( w != null ) try {
                w.close();
            } catch (IOException ignored) {}
        }
    }
}
