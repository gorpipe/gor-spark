package gorsat.spark;

import com.nextcode.spark.SparkGOR;
import gorsat.process.SparkRowSource;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

public abstract class GorBatchTable implements Table, SupportsRead, SupportsWrite, SupportsPushDownFilters {
    String path;
    String inputfilter;
    String filterFile;
    String splitFile;
    String fchrom;
    int fstart = 0;
    int fstop = -1;
    String redisUri;
    String jobId;
    String cacheFile;
    String useCpp;
    StructType schema = SparkGOR.gorrowEncoder().schema();

    public GorBatchTable(String path, String filter, String filterFile, String splitFile, String seek, String redisUri, String jobId, String cacheFile, String useCpp) throws IOException, DataFormatException {
        this.path = path;
        this.inputfilter = filter;
        this.filterFile = filterFile;
        this.splitFile = splitFile;
        this.redisUri = redisUri;
        this.jobId = jobId;
        this.cacheFile = cacheFile;
        this.useCpp = useCpp;
        checkSeek(seek);
        inferSchema();
    }

    public GorBatchTable(String path, String filter, String filterFile, String splitFile, String seek, StructType schema, String redisUri, String jobId, String cacheFile, String useCpp) {
        this.path = path;
        this.inputfilter = filter;
        this.schema = schema;
        this.filterFile = filterFile;
        this.splitFile = splitFile;
        this.redisUri = redisUri;
        this.jobId = jobId;
        this.cacheFile = cacheFile;
        this.useCpp = useCpp;
        checkSeek(seek);
    }

    void checkSeek(String seek) {
        if( seek != null && seek.length() > 0 ) {
            String[] spl = seek.split(":");
            fchrom = spl[0];
            if( spl.length > 1 ) {
                String[] sspl = spl[1].split("-");
                fstart = Integer.parseInt(sspl[0]);
                if( sspl.length > 1 ) {
                    fstop = Integer.parseInt(sspl[1]);
                }
            }
        }
    }

    void inferSchema() throws IOException, DataFormatException {
        Path ppath = Paths.get(path);
        schema = SparkRowSource.inferSchema(ppath, ppath.getFileName().toString(), false, path.toLowerCase().endsWith(".gorz"));
    }

    private static final Set<TableCapability> CAPABILITIES = new HashSet<>(Arrays.asList(
            TableCapability.BATCH_READ,
            //TableCap
            TableCapability.BATCH_WRITE,
            TableCapability.TRUNCATE));

    @Override
    public Scan build() {
        return null;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        return new Filter[0];
    }

    @Override
    public Filter[] pushedFilters() {
        return new Filter[0];
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public String name() {
        return this.getClass().toString();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public WriteBuilder newWriteBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return new GorWriteBuilder() {

        };
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        return new GorScanBuilder(schema, redisUri, jobId, cacheFile, useCpp) {
            Filter[] pushedFilters = new Filter[0];
            String filterChrom = fchrom;
            int start = fstart;
            int stop = fstop;
            String filter = GorBatchTable.this.inputfilter;

            @Override
            public Filter[] pushFilters(Filter[] filters) {
                Filter[] ret;
                if( schema.size() > 2 ) {
                    String posName = schema.fieldNames()[1];
                    String lastName = schema.fieldNames()[schema.size() - 1];

                    Optional<Filter> chrOpt = Arrays.stream(filters).filter(f -> f instanceof EqualTo).filter(f -> ((EqualTo) f).attribute().equalsIgnoreCase("CHROM")).findFirst();
                    chrOpt.map(f -> ((EqualTo) f).value().toString()).ifPresent(s -> filterChrom = s);

                    Optional<Filter> inOpt = Arrays.stream(filters).filter(f -> f instanceof In).filter(f -> ((In) f).attribute().equalsIgnoreCase(lastName)).findFirst();
                    if( !inOpt.isPresent() ) {
                        inOpt = Arrays.stream(filters).filter(f -> f instanceof EqualTo).filter(f -> ((EqualTo) f).attribute().equalsIgnoreCase(lastName)).findFirst();
                        inOpt.map(f -> ((EqualTo) f).value()).map(Object::toString).ifPresent(s -> filter = s);
                    } else {
                        inOpt.map(f -> Arrays.stream(((In) f).values()).map(Object::toString).collect(Collectors.joining(","))).ifPresent(s -> filter = s);
                    }

                    /* jdk9+ required
                    Optional<Filter> posGreatOpt = Arrays.stream(filters).filter(f -> f instanceof GreaterThan).filter(f -> ((GreaterThan) f).attribute().equalsIgnoreCase(posName)).findFirst().or(Arrays.stream(filters).filter(f -> f instanceof GreaterThanOrEqual).filter(f -> ((GreaterThanOrEqual) f).attribute().equalsIgnoreCase(posName))::findFirst);
                    posGreatOpt.ifPresent(f -> {
                        if (f instanceof GreaterThan) {
                            start = ((Number) ((GreaterThan) f).value()).intValue() - 1;
                        } else {
                            start = ((Number) ((LessThanOrEqual) f).value()).intValue();
                        }
                    });

                    Optional<Filter> posLessOpt = Arrays.stream(filters).filter(f -> f instanceof LessThan).filter(f -> ((LessThan) f).attribute().equalsIgnoreCase(posName)).findFirst().or(Arrays.stream(filters).filter(f -> f instanceof LessThanOrEqual).filter(f -> ((LessThanOrEqual) f).attribute().equalsIgnoreCase(posName))::findFirst);
                    posLessOpt.ifPresent(f -> {
                        if (f instanceof LessThan) {
                            stop = ((Number) ((LessThan) f).value()).intValue();
                        } else {
                            stop = ((Number) ((LessThanOrEqual) f).value()).intValue() + 1;
                        }
                    });*/

                    Optional<Filter> posGreatOpt = Arrays.stream(filters).filter(f -> f instanceof GreaterThan).filter(f -> ((GreaterThan) f).attribute().equalsIgnoreCase(posName)).findFirst();
                    if (!posGreatOpt.isPresent()) posGreatOpt = Arrays.stream(filters).filter(f -> f instanceof GreaterThanOrEqual).filter(f -> ((GreaterThanOrEqual) f).attribute().equalsIgnoreCase(posName)).findFirst();
                    posGreatOpt.ifPresent(f -> {
                        if (f instanceof GreaterThan) {
                            start = ((Number) ((GreaterThan) f).value()).intValue() - 1;
                        } else {
                            start = ((Number) ((LessThanOrEqual) f).value()).intValue();
                        }
                    });
                    final Optional<Filter> posGreatOptFinal = posGreatOpt;

                    Optional<Filter> posLessOpt = Arrays.stream(filters).filter(f -> f instanceof LessThan).filter(f -> ((LessThan) f).attribute().equalsIgnoreCase(posName)).findFirst();
                    if (!posLessOpt.isPresent()) posLessOpt = Arrays.stream(filters).filter(f -> f instanceof LessThanOrEqual).filter(f -> ((LessThanOrEqual) f).attribute().equalsIgnoreCase(posName)).findFirst();
                    posLessOpt.ifPresent(f -> {
                        if (f instanceof LessThan) {
                            stop = ((Number) ((LessThan) f).value()).intValue();
                        } else {
                            stop = ((Number) ((LessThanOrEqual) f).value()).intValue() + 1;
                        }
                    });
                    final Optional<Filter> posLessOptFinal = posLessOpt;

                    ret =  Arrays.stream(filters).filter(f -> (!chrOpt.isPresent() || !f.equals(chrOpt.get())) && (!posGreatOptFinal.isPresent() || !f.equals(posGreatOptFinal.get())) && (!posLessOptFinal.isPresent() || !f.equals(posLessOptFinal.get()))).toArray(Filter[]::new);
                } else ret = new Filter[0];

                Set<Filter> fset = new HashSet<>(Arrays.asList(ret));
                pushedFilters = Arrays.stream(filters).filter(f -> !fset.contains(f)).toArray(Filter[]::new);
                return ret;
                //return filters;
            }

            @Override
            public Filter[] pushedFilters() {
                return pushedFilters;
            }

            @Override
            public InputPartition[] planInputPartitions() {
                InputPartition[] partitions = null;
                if( filterChrom != null ) {
                    partitions = new InputPartition[1];
                    partitions[0] = new GorRangeInputPartition(path, filter, filterFile, filterChrom, start, stop, filterChrom);
                } else {
                    if (splitFile != null) {
                        try {
                            partitions = Files.lines(Paths.get(splitFile)).skip(1).map(line -> line.split("\t")).map(s -> new GorRangeInputPartition(path, filter, filterFile, s[0], Integer.parseInt(s[1]), Integer.parseInt(s[2]), s.length > 3 ? s[3] : s[0] + ":" + s[1] + "-" + s[2])).toArray(InputPartition[]::new);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    if (partitions == null) {
                        partitions = new InputPartition[24];
                        partitions[0] = new GorRangeInputPartition(path, filter, filterFile, "chr1", 0, 249250621, "chr1");
                        partitions[1] = new GorRangeInputPartition(path, filter, filterFile, "chr10", 0, 135534747, "chr10");
                        partitions[2] = new GorRangeInputPartition(path, filter, filterFile, "chr11", 0, 135006516, "chr11");
                        partitions[3] = new GorRangeInputPartition(path, filter, filterFile, "chr12", 0, 133851895, "chr12");
                        partitions[4] = new GorRangeInputPartition(path, filter, filterFile, "chr13", 0, 115169878, "chr13");
                        partitions[5] = new GorRangeInputPartition(path, filter, filterFile, "chr14", 0, 107349540, "chr14");
                        partitions[6] = new GorRangeInputPartition(path, filter, filterFile, "chr15", 0, 102531392, "chr15");
                        partitions[7] = new GorRangeInputPartition(path, filter, filterFile, "chr16", 0, 90354753, "chr16");
                        partitions[8] = new GorRangeInputPartition(path, filter, filterFile, "chr17", 0, 81195210, "chr17");
                        partitions[9] = new GorRangeInputPartition(path, filter, filterFile, "chr18", 0, 78077248, "chr18");
                        partitions[10] = new GorRangeInputPartition(path, filter, filterFile, "chr19", 0, 59128983, "chr19");
                        partitions[11] = new GorRangeInputPartition(path, filter, filterFile, "chr2", 0, 243199373, "chr2");
                        partitions[12] = new GorRangeInputPartition(path, filter, filterFile, "chr20", 0, 63025520, "chr20");
                        partitions[13] = new GorRangeInputPartition(path, filter, filterFile, "chr21", 0, 48129895, "chr21");
                        partitions[14] = new GorRangeInputPartition(path, filter, filterFile, "chr22", 0, 51304566, "chr22");
                        partitions[15] = new GorRangeInputPartition(path, filter, filterFile, "chr3", 0, 198022430, "chr3");
                        partitions[16] = new GorRangeInputPartition(path, filter, filterFile, "chr4", 0, 191154276, "chr4");
                        partitions[17] = new GorRangeInputPartition(path, filter, filterFile, "chr5", 0, 180915260, "chr5");
                        partitions[18] = new GorRangeInputPartition(path, filter, filterFile, "chr6", 0, 171115067, "chr6");
                        partitions[19] = new GorRangeInputPartition(path, filter, filterFile, "chr7", 0, 159138663, "chr7");
                        partitions[20] = new GorRangeInputPartition(path, filter, filterFile, "chr8", 0, 146364022, "chr8");
                        partitions[21] = new GorRangeInputPartition(path, filter, filterFile, "chr9", 0, 141213431, "chr9");
                        partitions[22] = new GorRangeInputPartition(path, filter, filterFile, "chrX", 0, 155270560, "chrX");
                        partitions[23] = new GorRangeInputPartition(path, filter, filterFile, "chrY", 0, 59373566, "chrY");
                    }
                }
                return partitions;
            }
        };
    }

    @Override
    public Transform[] partitioning() {
        return new Transform[] {
            Expressions.identity("i")
        };
    }

    /*@Override
    public String name() {
        return null;
    }

    @Override
    public StructType schema() {
        return null;
    }

    @Override
    public Transform[] partitioning() {
        return new Transform[0];
    }

    @Override
    public Map<String, String> properties() {
        return null;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return null;
    }*/
}
