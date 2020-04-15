package gorsat.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.gorpipe.spark.GorSparkSession;
import gorsat.BatchedReadSource;
import gorsat.process.GorPipe;
import gorsat.process.PipeInstance;
import gorsat.process.PipeOptions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.gorpipe.model.gor.iterators.RowSource;
import org.gorpipe.spark.SparkGorMonitor;
import org.gorpipe.spark.SparkGorRow;
import org.gorpipe.spark.SparkSessionFactory;
import org.gorpipe.spark.platform.JobField;


import static org.apache.spark.sql.types.DataTypes.*;

public class GorReaderFactory implements PartitionReaderFactory {
    StructType schema;
    String redisUri;
    String jobId;
    String cacheFile;
    String useCpp;

    public GorReaderFactory(StructType schema, String redisUri, String jobId, String cacheFile, String useCpp) {
        this.schema = schema;
        this.redisUri = redisUri;
        this.jobId = jobId;
        this.cacheFile = cacheFile;
        this.useCpp = useCpp;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        GorRangeInputPartition p = (GorRangeInputPartition) partition;
        PartitionReader<InternalRow> partitionReader = useCpp != null && useCpp.equalsIgnoreCase("blue") ? new PartitionReader<InternalRow>() {
            BufferedReader br;
            String currentLine;
            StructField[] fields = schema.fields();
            GenericInternalRow gir = new GenericInternalRow(new Object[fields.length]);

            @Override
            public boolean next() {
                try {
                    if(br==null) {
                        ProcessBuilder processBuilder = new ProcessBuilder("cgor", "-p", p.chr, p.path);
                        Process process = processBuilder.start();
                        InputStream is = process.getInputStream();
                        InputStreamReader isr = new InputStreamReader(is);
                        br = new BufferedReader(isr);
                        br.readLine();
                    }
                    currentLine = br.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return currentLine != null;
            }

            @Override
            public InternalRow get() {
                int start = 0;
                int i = 0;
                for(; i < fields.length-1; i++) {
                    int last = currentLine.indexOf('\t', start+1);
                    String str = currentLine.substring(start, last);
                    StructField sf = fields[i];
                    if(sf.dataType() == StringType) {
                        gir.update(i, UTF8String.fromString(str));
                    } else if(sf.dataType() == IntegerType) {
                        gir.update(i, Integer.parseInt(str));
                    } else if(sf.dataType() == DoubleType) {
                        gir.update(i, Double.parseDouble(str));
                    } else if(sf.dataType() == LongType) {
                        gir.update(i, Long.parseLong(str));
                    }
                    start = last+1;
                }

                int last = currentLine.length();
                String str = currentLine.substring(start, last);
                StructField sf = fields[i];
                if(sf.dataType() == StringType) {
                    gir.update(i, UTF8String.fromString(str));
                } else if(sf.dataType() == IntegerType) {
                    gir.update(i, Integer.parseInt(str));
                } else if(sf.dataType() == DoubleType) {
                    gir.update(i, Double.parseDouble(str));
                } else if(sf.dataType() == LongType) {
                    gir.update(i, Long.parseLong(str));
                }

                return gir;
            }

            @Override
            public void close() throws IOException {
                br.close();
            }
        } : new PartitionReader<InternalRow>() {
            RowSource iterator;
            SparkGorRow sparkRow = new SparkGorRow(schema);
            SparkGorMonitor sparkGorMonitor;

            @Override
            public boolean next() {
                if( iterator == null ) {
                    sparkGorMonitor = new SparkGorMonitor(redisUri,jobId) {
                        @Override
                        public boolean isCancelled() {
                            return sparkGorMonitor.getValue(JobField.CancelFlag) != null;
                        }
                    };
                    SparkSessionFactory sessionFactory = new SparkSessionFactory(null, Paths.get(".").toAbsolutePath().normalize().toString(), "result_cache", sparkGorMonitor);
                    GorSparkSession gorPipeSession = (GorSparkSession) sessionFactory.create();
                    PipeInstance pi = new PipeInstance(gorPipeSession.getGorContext());

                    boolean useNative = useCpp != null && useCpp.equalsIgnoreCase("true");
                    String seek = useNative ? "cmd " : "gor ";

                    Path epath = Paths.get(p.path);
                    String epathstr;
                    if(Files.isDirectory(epath)) {
                        try {
                            epathstr = Files.walk(epath).skip(1).map(Path::toString).filter(p -> p.endsWith(".gorz")).collect(Collectors.joining(" "));
                        } catch (IOException e) {
                            epathstr = p.path;
                        }
                    } else {
                        epathstr = p.path;
                    }
                    String spath = useNative ? "cgor #(S:-p chr:pos) "+p.path+"}" : epathstr;
                    String s = p.filterColumn != null && p.filterColumn.length() > 0 ? "-s "+p.filterColumn + " " : "";
                    String path = seek + (p.filterFile == null ? p.filter == null ? s + spath : s + "-f " + p.filter + " " + spath : s + "-ff " + p.filterFile + " " + spath);
                    String[] args = {path};
                    PipeOptions options = new PipeOptions();
                    options.parseOptions(args);
                    pi.subProcessArguments(options);

                    RowSource rowSource = pi.theInputSource();
                    rowSource.setPosition(p.chr, p.start);

                    if (redisUri != null && redisUri.length() > 0) {
                        iterator = new BatchedReadSource(rowSource, GorPipe.brsConfig(), rowSource.getHeader(), sparkGorMonitor);
                    } else {
                        iterator = rowSource;
                    }
                }
                boolean hasNext = iterator.hasNext();
                if( hasNext ) {
                    org.gorpipe.model.genome.files.gor.Row gorrow = iterator.next();
                    if( p.tag != null ) gorrow = gorrow.rowWithAddedColumn(p.tag);
                    hasNext = gorrow.chr.equals(p.chr) && (p.end == -1 || gorrow.pos <= p.end);
                    sparkRow.row = gorrow;
                }
                return hasNext;
            }

            @Override
            public InternalRow get() {
                return encoder.toRow(sparkRow);
            }

            @Override
            public void close() {
                if(iterator != null) iterator.close();
            }
        };
        return partitionReader;
    }
}