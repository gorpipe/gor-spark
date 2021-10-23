package gorsat.spark;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.binsearch.Unzipper;
import org.gorpipe.model.gor.RowObj;
import org.gorpipe.spark.SparkGorRow;
import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.CollectionConverters;

class GorzFunction implements Function1<PartitionedFile, Iterator<InternalRow>>, Serializable {
    Function1<PartitionedFile, Iterator<InternalRow>> func;
    ExpressionEncoder<Row> encoder;
    ExpressionEncoder.Serializer<Row> serializer;
    Unzipper unzip;
    String chrom;
    int start;
    int stop;
    byte[] unzipped;

    GorzFunction(Function1 func, StructType schema, Collection<Filter> filters) {
        this.func = func;
        this.unzipped = new byte[1<<17];

        List<Attribute> lattr = CollectionConverters.SeqHasAsJava(schema.toAttributes()).asJava().stream().map(Attribute::toAttribute).collect(Collectors.toList());
        var sattr = CollectionConverters.ListHasAsScala(lattr).asScala().toSeq();

        this.encoder = RowEncoder.apply(schema).resolveAndBind(sattr, SimpleAnalyzer$.MODULE$);
        this.serializer = encoder.createSerializer();
        //this.unzip = new Unzipper(false);
        this.chrom = filters.stream().filter(f -> f instanceof EqualTo).map(f -> (EqualTo)f).filter(f -> f.attribute().equalsIgnoreCase("chrom")).map(EqualTo::value).map(Object::toString).findFirst().orElse(null);
        this.start = filters.stream().filter(f -> f instanceof GreaterThan).map(f -> (GreaterThan)f).filter(f -> f.attribute().equalsIgnoreCase("pos")).map(GreaterThan::value).map(f -> (Integer)f).findFirst().orElse(-1);
        this.stop = filters.stream().filter(f -> f instanceof LessThan).map(f -> (LessThan)f).filter(f -> f.attribute().equalsIgnoreCase("pos")).map(LessThan::value).map(f -> (Integer)f).findFirst().orElse(-1);
    }

    @Override
    public Iterator<InternalRow> apply(PartitionedFile v1) {
        Iterator<InternalRow> it = func.apply(v1);
        Stream<String> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(CollectionConverters.IteratorHasAsJava(it).asJava(), Spliterator.ORDERED), false).map(ir -> ir.getString(0));

        // Do not remove, reinsert when start building with jdk 11+
        /*stream = chrom != null ? stream.dropWhile(f -> {
            int i = f.indexOf('\t');
            return chrom.compareTo(f.substring(0, i)) > 0;
        }).takeWhile(f -> {
            int i = f.indexOf('\t');
            return chrom.equals(f.substring(0, i));
        }) : stream;

        stream = start != -1 ? stream.dropWhile(f -> {
            int i = f.indexOf('\t')+1;
            int e = f.indexOf('\t',i);
            return start > Integer.parseInt(f.substring(i, e));
        }) : stream;

        stream = stream.map(res -> {
            int i = res.indexOf('\t');
            i = res.indexOf('\t', i+1);
            return res.substring(i+2);
        });

        stream = stream.flatMap(s -> {
            byte[] bb = Base64.getDecoder().decode(s);
            unzip.setType(CompressionType.ZLIB);
            unzip.setRawInput(bb,0,bb.length);
            int unzipLen = 0; //unzipToNewBuffer(bb, 0, bb.length, (byte)0, null);
            try {
                unzipLen = unzip.decompress(unzipped,0,unzipped.length);
            } catch (DataFormatException | IOException e) {
                throw new GorSystemException("gorz write failed",e);
            }
            ByteArrayInputStream bais = new ByteArrayInputStream(unzipped,0,unzipLen);
            InputStreamReader isr = new InputStreamReader(bais);
            BufferedReader br = new BufferedReader(isr);
            return br.lines();
        });

        stream = start != -1 ? stream.dropWhile(f -> {
            int i = f.indexOf('\t')+1;
            int e = f.indexOf('\t',i);
            return start > Integer.parseInt(f.substring(i, e));
        }) : stream;

        stream = stop != -1 ? stream.takeWhile(f -> {
            int i = f.indexOf('\t')+1;
            int e = f.indexOf('\t',i);
            return stop > Integer.parseInt(f.substring(i, e));
        }) : stream;*/

        Stream<InternalRow> istream = stream.map(RowObj::apply).map(r -> new SparkGorRow(r, encoder.schema())).map(r -> serializer.apply(r).copy());
        java.util.Iterator<InternalRow> iterator = istream.iterator();
        return CollectionConverters.IteratorHasAsScala(iterator).asScala();
    }

    @Override
    public <A> Function1<A, Iterator<InternalRow>> compose(Function1<A, PartitionedFile> g) {
        return func.compose(g);
    }

    @Override
    public <A> Function1<PartitionedFile, A> andThen(Function1<Iterator<InternalRow>, A> g) {
        return func.andThen(g);
    }
}