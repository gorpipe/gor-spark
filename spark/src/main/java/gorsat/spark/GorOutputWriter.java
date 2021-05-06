package gorsat.spark;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.binsearch.GorIndexType;
import org.gorpipe.spark.GorSparkRow;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.Deflater;

public class GorOutputWriter extends OutputWriter {
    GORzip of;
    ExpressionEncoder.Deserializer<Row> deserializer;
    Path path;
    String originalPath;

    public GorOutputWriter(String uristr, StructType schema, String originalPath) throws IOException {
        this.originalPath = originalPath;
        List<Attribute> lattr = JavaConverters.asJavaCollection(schema.toAttributes()).stream().map(Attribute::toAttribute).collect(Collectors.toList());
        Seq sattr = JavaConverters.asScalaBuffer(lattr).toSeq();
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema).resolveAndBind(sattr, SimpleAnalyzer$.MODULE$);
        deserializer = encoder.createDeserializer();
        String header = String.join("\t", schema.fieldNames());
        URI uri = URI.create(uristr);
        path = new Path(uri);
        of = new GORzip(path, header, false, false, GorIndexType.NONE, Deflater.BEST_SPEED, "");
        of.setup();
    }

    @Override
    public void write(InternalRow row) {
        Row grow = deserializer.apply(row);
        org.gorpipe.gor.model.Row sparkrow = new GorSparkRow(grow);
        of.process(sparkrow);
    }

    @Override
    public void close() {
        of.finish();

        //Path origPath = Paths.get(originalPath);
        //w = Files.newBufferedWriter(origPath.getParent().resolve(origPath.getFileName().toString()+".gord"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        //w.write(origPath.resolve(path.getFileName()) + "\t1\t" + startChrom + "\t" + startPos + "\t" + stopChrom + "\t" + stopPos + '\n');
        /*Writer w = null;
        if( w != null ) try {
            w.close();
        } catch (IOException ignored) {}*/
    }
}
