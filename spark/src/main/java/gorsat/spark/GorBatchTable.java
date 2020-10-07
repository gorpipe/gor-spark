package gorsat.spark;

import gorsat.Script.ScriptEngineFactory;
import gorsat.Script.ScriptExecutionEngine;
import gorsat.process.GorDataType;
import gorsat.process.SparkRowUtilities;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.gorpipe.gor.reference.ReferenceBuildDefaults;
import org.gorpipe.spark.GorSparkSession;
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
import org.gorpipe.spark.SparkSessionFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;

import static org.apache.spark.sql.types.DataTypes.StringType;

public abstract class GorBatchTable implements Table, SupportsRead, SupportsWrite, SupportsPushDownFilters {
    String[] commands;
    String query;
    String path;
    String inputfilter;
    String filterFile;
    String filterColumn;
    String splitFile;
    String fchrom;
    int fstart = 0;
    int fstop = -1;
    String redisUri;
    String jobId;
    String cacheFile;
    String useCpp;
    StructType schema;
    boolean tag;
    String projectRoot;
    String cacheDir;

    public GorBatchTable(String query, boolean tag, String path, String filter, String filterFile, String filterColumn, String splitFile, String seek, String redisUri, String jobId, String cacheFile, String useCpp) {
        init(query,tag,path,filter,filterFile,filterColumn,splitFile,seek,redisUri,jobId,cacheFile,useCpp);
    }

    public GorBatchTable(String query, boolean tag, String path, String filter, String filterFile, String filterColumn, String splitFile, String seek, StructType schema, String redisUri, String jobId, String cacheFile, String useCpp) {
        init(query,tag,path,filter,filterFile,filterColumn,splitFile,seek,redisUri,jobId,cacheFile,useCpp);
        this.schema = schema;
    }

    public void setProjectRoot(String projectRoot) {
        this.projectRoot = projectRoot;
    }

    public void setCacheDir(String cacheDir) {
        this.cacheDir = cacheDir;
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

    void init(String query, boolean tag, String path, String filter, String filterFile, String filterColumn, String splitFile, String seek, String redisUri, String jobId, String cacheFile, String useCpp) {
        this.query = query;
        this.projectRoot = Paths.get(".").toAbsolutePath().normalize().toString();
        this.cacheDir = "result_cache";
        this.tag = tag;
        this.path = path;
        this.inputfilter = filter;
        this.filterFile = filterFile;
        this.filterColumn = filterColumn;
        this.splitFile = splitFile;
        this.redisUri = redisUri;
        this.jobId = jobId;
        this.cacheFile = cacheFile;
        this.useCpp = useCpp;
        checkSeek(seek);
    }

    private String[] initCommands(String query) {
        String[] commands = null;
        if(query!=null) {
            if(query.toLowerCase().startsWith("pgor") || query.toLowerCase().startsWith("partgor") || query.toLowerCase().startsWith("parallel")) {
                ReceiveQueryHandler receiveQueryHandler = new ReceiveQueryHandler();
                SparkSessionFactory sessionFactory = new SparkSessionFactory(null, projectRoot, cacheDir, null, receiveQueryHandler);
                GorSparkSession gorPipeSession = (GorSparkSession) sessionFactory.create();
                ScriptExecutionEngine see = ScriptEngineFactory.create(gorPipeSession.getGorContext());
                see.execute(new String[]{query}, false);
                commands = receiveQueryHandler.getCommandsToExecute();
            } else commands = new String[] {query};
        }
        return commands;
    }

    void inferSchema() {
        commands = initCommands(query);
        schema = Encoders.STRING().schema();
        if(path!=null) {
            Path ppath = Paths.get(path);
            try {
                schema = SparkRowUtilities.inferSchema(ppath, ppath.getFileName().toString(), false, path.toLowerCase().endsWith(".gorz"));
            } catch (IOException | DataFormatException e) {
                throw new RuntimeException("Unable to infer schema from "+ppath.toString(), e);
            }
        } else if(commands!=null) {
            String query = commands[0];
            boolean nor = query.toLowerCase().startsWith("nor ");
            SparkSessionFactory sessionFactory = new SparkSessionFactory(null, projectRoot, cacheDir, null);
            GorSparkSession gorPipeSession = (GorSparkSession) sessionFactory.create();
            GorDataType gdt = SparkRowUtilities.gorCmdSchema(query,gorPipeSession, nor);

            String[] headerArray = gdt.header;
            DataType[] dataTypes = new DataType[headerArray.length];
            int start = 0;
            for (int i = start; i < dataTypes.length; i++) {
                dataTypes[i] = gdt.dataTypeMap.getOrDefault(i, StringType);
            }

            Stream<StructField> fieldStream = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty()));
            StructField[] fields = (tag ? Stream.concat(fieldStream,Stream.of(new StructField("Tag", StringType, true, Metadata.empty()))) : fieldStream).toArray(StructField[]::new);
            schema = new StructType(fields);
        }
    }

    private static final Set<TableCapability> CAPABILITIES = new HashSet<>(Arrays.asList(
            TableCapability.BATCH_READ,
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
        if(schema==null) inferSchema();
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
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new GorWriteBuilder() {};
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        if(schema==null) inferSchema();
        return new GorScanBuilder(schema, redisUri, jobId, cacheFile, projectRoot, cacheDir, useCpp) {
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

                    ret =  Arrays.stream(filters).filter(f -> !chrOpt.isPresent() || (!f.equals(chrOpt.get()) && (!posGreatOptFinal.isPresent() || !f.equals(posGreatOptFinal.get())) && (!posLessOptFinal.isPresent() || !f.equals(posLessOptFinal.get())))).toArray(Filter[]::new);
                } else ret = new Filter[0];

                Set<Filter> fset = new HashSet<>(Arrays.asList(ret));
                pushedFilters = Arrays.stream(filters).filter(f -> !fset.contains(f)).toArray(Filter[]::new);
                return ret;
            }

            @Override
            public Filter[] pushedFilters() {
                return pushedFilters;
            }

            @Override
            public InputPartition[] planInputPartitions() {
                InputPartition[] partitions = null;
                if( commands != null ) {
                    partitions = Arrays.stream(commands).map(cmd -> {
                        String tagstr = null;
                        if(tag) {
                            int i = cmd.indexOf("-p ") + 3;
                            if (i != -1) {
                                while (i < cmd.length() && cmd.charAt(i) == ' ') i++;
                                int k = i + 1;
                                while (k < cmd.length() && cmd.charAt(k) != ' ') k++;
                                tagstr = cmd.substring(i, k);
                            }
                        }
                        return new GorRangeInputPartition(cmd, tagstr);
                    }).toArray(GorRangeInputPartition[]::new);
                } else if( filterChrom != null ) {
                    partitions = new InputPartition[1];
                    partitions[0] = new GorRangeInputPartition(path, filter, filterFile, filterColumn, filterChrom, start, stop, filterChrom);
                } else {
                    if (splitFile != null) {
                        try {
                            partitions = Files.lines(Paths.get(splitFile)).skip(1).map(line -> line.split("\t")).map(s -> new GorRangeInputPartition(path, filter, filterFile, filterColumn, s[0], Integer.parseInt(s[1]), Integer.parseInt(s[2]), s.length > 3 ? s[3] : s[0] + ":" + s[1] + "-" + s[2])).toArray(InputPartition[]::new);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    if (partitions == null) {
                        Map<String,Integer> buildSizeGeneric = ReferenceBuildDefaults.buildSizeGeneric();
                        partitions = buildSizeGeneric.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).map(e -> new GorRangeInputPartition(path, filter, filterFile, filterColumn,e.getKey(), 0, e.getValue(), e.getKey())).toArray(InputPartition[]::new);
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
