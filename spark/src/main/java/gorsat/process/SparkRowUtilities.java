package gorsat.process;

import gorsat.Commands.CommandParseUtilities;
import gorsat.DynIterator;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import org.gorpipe.gor.binsearch.CompressionType;
import org.gorpipe.gor.binsearch.Unzipper;
import org.gorpipe.gor.driver.GorDriverFactory;
import org.gorpipe.gor.driver.meta.SourceReference;
import org.gorpipe.gor.driver.providers.stream.sources.StreamSource;
import org.gorpipe.gor.model.ParquetLine;
import org.gorpipe.gor.model.Row;
import org.gorpipe.spark.GorSparkSession;
import org.gorpipe.spark.RowDataType;
import org.gorpipe.spark.RowGorRDD;
import org.gorpipe.util.collection.ByteArray;
import scala.collection.JavaConverters;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;

import static org.apache.spark.sql.types.DataTypes.*;

public class SparkRowUtilities {
    static final String csvDataSource = "csv";
    static final String gordatasourceClassname = "gorsat.spark.GorDataSource";

    public static String createMapString(Map<String,String> createMap, Map<String,String> defMap, String creates) {
        String mcreates = createMap.size() > 0 ? createMap.entrySet().stream().map(e -> "create "+e.getKey()+" = "+e.getValue()).collect(Collectors.joining("; ","",";")) : "";
        String mdefs = defMap.size() > 0 ? defMap.entrySet().stream().map(e -> "def "+e.getKey()+" = "+e.getValue()).collect(Collectors.joining("; ","",";")) : "";
        return mdefs + mcreates + creates;
    }

    public static List<String> createMapList(Map<String,String> createMap, Map<String,String> defMap, String creates) {
        List<String> lcreates = createMap.entrySet().stream().map(e -> "create "+e.getKey()+" = "+e.getValue()).collect(Collectors.toList());
        List<String> ldefs = defMap.entrySet().stream().map(e -> "def "+e.getKey()+" = "+e.getValue()).collect(Collectors.toList());
        List<String> lall = Arrays.asList(CommandParseUtilities.quoteSafeSplitAndTrim(creates, ';'));
        List<String> alist = new ArrayList<>();
        alist.addAll(ldefs);
        alist.addAll(lcreates);
        alist.addAll(lall);
        return alist;
    }

    public static String generateTempViewName(String fileName, boolean usegorpipe, String filter, String chr, int pos, int end) {
        String fixName = fileName;
        String prekey = usegorpipe + fixName;
        String key = filter == null ? prekey : filter + prekey;
        String ret = chr == null ? key : chr + pos + end + key;
        return "g" + Math.abs(ret.hashCode());
    }

    public static StructType gor2Schema(String header, Row types) {
        String[] hsplit = header.split("\t");
        StructField[] fields = new StructField[types.numCols()];
        for (int i = 0; i < fields.length; i++) {
            String type = types.stringValue(i);
            DataType dt;
            if (type.equals("S")) dt = StringType;
            else if (type.equals("D")) dt = DoubleType;
            else dt = IntegerType;
            fields[i] = new StructField(hsplit[i], dt, true, Metadata.empty());
        }
        return new StructType(fields);
    }

    public static StructType inferSchema(Path filePath, String fileName, boolean nor, boolean isGorz) throws IOException, DataFormatException {
        GorDataType gorDataType = inferDataTypes(filePath, fileName, isGorz, nor);
        String[] headerArray = gorDataType.header;
        Map<Integer, DataType> dataTypeMap = gorDataType.dataTypeMap;

        DataType[] dataTypes = new DataType[headerArray.length];
        int start = 0;
        if (!nor) {
            dataTypes[0] = StringType;
            dataTypes[1] = IntegerType;
            start = 2;
        }
        for (int i = start; i < dataTypes.length; i++) {
            dataTypes[i] = dataTypeMap.getOrDefault(i, StringType);
        }

        StructField[] fields = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty())).toArray(StructField[]::new);
        return new StructType(fields);
    }

    public static String translatePath(String fn, Path fileroot, String standalone) {
        String fileName;
        if (fn.contains("://")) {
            fileName = fn;
        } else {
            Path filePath = Paths.get(fn);
            if (!filePath.isAbsolute()) {
                if (standalone != null && standalone.length() > 0) {
                    int k = standalone.indexOf(' ');
                    if (k == -1) k = standalone.length();
                    filePath = Paths.get(standalone.substring(0, k)).resolve(fn);
                } else {
                    filePath = Paths.get(fn);
                    if (!filePath.isAbsolute() && !Files.exists(filePath)) {
                        filePath = fileroot.resolve(filePath).normalize().toAbsolutePath();
                    }
                }
            }
            fileName = filePath.toString();
        }
        return fileName;
    }

    public static GorDataType gorCmdSchema(String gorcmd, GorSparkSession gorSparkSession, boolean nor) {
        DynIterator.DynamicRowSource drs = new DynIterator.DynamicRowSource(gorcmd, gorSparkSession.getGorContext(), false);
        String header = drs.getHeader();
        String[] ha = header.split("\t");
        Stream<String> linestream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(drs, Spliterator.ORDERED), false).map(Object::toString).onClose(drs::close);
        GorDataType gdt = typeFromStream(linestream, false, ha, nor);
        gdt.setUsedFiles(JavaConverters.seqAsJavaList(drs.usedFiles()));
        return gdt;
    }

    public static GorDataType gorCmdSchema(String[] gorcmds, GorSparkSession gorSparkSession) {
        Stream<DynIterator.DynamicRowSource> sdrs = Arrays.stream(gorcmds).map(d -> new DynIterator.DynamicRowSource(d, gorSparkSession.getGorContext(), false));
        Stream<Stream<String>> lstr = sdrs.map(drs -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(drs, Spliterator.ORDERED), false).map(Object::toString).onClose(drs::close));

        String query = gorcmds[0];
        boolean nor = query.toLowerCase().startsWith("nor ") || query.toLowerCase().startsWith("norrows ");
        DynIterator.DynamicRowSource drs = new DynIterator.DynamicRowSource(query, gorSparkSession.getGorContext(), false);
        String header = drs.getHeader();
        List<String> usedFiles = JavaConverters.seqAsJavaList(drs.usedFiles());
        drs.close();
        String[] ha = header.split("\t");

        Stream<String> linestream = lstr.reduce(Stream::concat).get();
        GorDataType gdt = typeFromStream(linestream, false, ha, nor);
        gdt.setUsedFiles(usedFiles);
        return gdt;
    }

    public static Dataset<? extends org.apache.spark.sql.Row> registerFile(String[] fns, String name, String profile, GorSparkSession gorSparkSession, String standalone, Path fileroot, Path cacheDir, boolean usestreaming, String filter, String filterFile, String filterColumn, String splitFile, final boolean nor, final String chr, final int pos, final int end, final String jobid, String cacheFile, boolean cpp, boolean tag, StructType schema) throws IOException, DataFormatException {
        String fn = fns[0];
        boolean curlyQuery = fn.startsWith("{");
        boolean nestedQuery = fn.startsWith("<(") || curlyQuery;
        Path filePath = null;
        String fileName;
        if (nestedQuery) {
            fileName = fn.substring(curlyQuery ? 1 : 2, fn.length() - 1);
        } else {
            fileName = translatePath(fn, fileroot, standalone);
            filePath = Paths.get(fileName);
        }
        String tempViewName = generateTempViewName(fileName, usestreaming, filter, chr, pos, end);

        Map<Integer, DataType> dataTypeMap;
        DataType[] dataTypes;
        Dataset<? extends org.apache.spark.sql.Row> gor;
        String[] tableNames = gorSparkSession.getSparkSession().sqlContext().tableNames();
        if (gorSparkSession.datasetMap().containsKey(tempViewName) && Arrays.asList(tableNames).contains(tempViewName)) {
            RowDataType rdt = gorSparkSession.datasetMap().get(tempViewName);
            gor = rdt.dataset;
            dataTypes = rdt.datatypes;

            dataTypeMap = new HashMap<>();
            IntStream.range(0, dataTypes.length).forEach(i -> {
                DataType dt = dataTypes[i];
                if (dt != StringType) dataTypeMap.put(i, dt);
            });
            if (name != null) gor.createOrReplaceTempView(name);
        } else {
            nestedQuery = false; //!fn.startsWith("<(spark") && !fn.startsWith("<(pgor ") && !fn.startsWith("<(partgor ") && !fn.startsWith("<(parallel ") || !fn.startsWith("<(gor ");
            if (nestedQuery) {
                boolean hasFilter = filter != null && filter.length() > 0;
                String gorcmd = fileName;
                if (hasFilter) {
                    gorcmd = gorcmd.substring(0, 4) + "-f" + filter + gorcmd.substring(3);
                }
                if (chr != null) {
                    String rest = gorcmd.substring(3);
                    gorcmd = gorcmd.substring(0, 4) + "-p" + chr + ":" + pos + "-";
                    if (end != -1) gorcmd += end;
                    gorcmd += rest;
                }

                GorDataType gdt = gorCmdSchema(gorcmd, gorSparkSession, nor);

                String[] headerArray;
                boolean isGord = false;
                List<String> usedFiles = gdt.usedFiles;
                if (usedFiles.size() > 0) {
                    fileName = usedFiles.get(0);
                    if (!fileName.contains("://")) {
                        filePath = standalone != null && standalone.length() > 0 ? Paths.get(standalone).resolve(fileName) : Paths.get(fileName);
                    }
                    isGord = fileName.toLowerCase().endsWith(".gord");
                    if (isGord && !hasFilter) {
                        headerArray = Arrays.copyOf(gdt.header, gdt.header.length + 1);
                        headerArray[headerArray.length - 1] = "PN";
                    } else headerArray = gdt.header;
                } else headerArray = gdt.header;

                dataTypes = new DataType[headerArray.length];
                int start = 0;
                if (!nor) {
                    dataTypes[0] = StringType;
                    dataTypes[1] = IntegerType;
                    start = 2;
                }
                for (int i = start; i < dataTypes.length; i++) {
                    dataTypes[i] = gdt.dataTypeMap.getOrDefault(i, StringType);
                }

                StructField[] fields = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty())).toArray(StructField[]::new);
                schema = new StructType(fields);

                ExpressionEncoder encoder = RowEncoder.apply(schema);
                Map<Path, String> fNames;
                Map<String, String> uNames;
                if (isGord) {
                    assert filePath != null;
                    Path fileParent = filePath.getParent();
                    fNames = Files.lines(filePath).map(l -> l.split("\t")).peek(l -> l[0] = l[0].split("\\|")[0]).collect(Collectors.toMap(s -> fileParent.resolve(s[0]), s -> s[1]));
                    uNames = new HashMap<>();
                    for (Path p : fNames.keySet()) {
                        uNames.put(p.toUri().toString(), fNames.get(p));
                    }
                } else {
                    fNames = null;
                }

                JavaRDD<Row> rdd = new RowGorRDD(gorSparkSession.getSparkSession(), gorcmd, "", !hasFilter && fNames != null ? String.join(",", fNames.values()) : null, chr, pos, end, true).toJavaRDD();

                Function<Row, org.apache.spark.sql.Row> rfunc = (Function<Row, org.apache.spark.sql.Row>) a -> {
                    Object[] o = new Object[a.numCols()];
                    o[0] = a.chr;
                    o[1] = a.pos;
                    for (int i = 2; i < o.length; i++) {
                        if (fields[i].dataType().sameType(IntegerType)) o[i] = a.colAsInt(i);
                        else if (fields[i].dataType().sameType(DoubleType)) o[i] = a.colAsDouble(i);
                        else o[i] = a.colAsString(i).toString();
                    }
                    return RowFactory.create(o);
                };
                JavaRDD nrdd = rdd.map(rfunc);
                gor = gorSparkSession.getSparkSession().createDataset(nrdd.rdd(), encoder);
            } else {
                boolean isGord = fileName.toLowerCase().endsWith(".gord");
                Map<Path, String> fNames;
                Map<String, String> uNames;
                Path dictFile = null;
                int dictSplit = 0;
                if (isGord && filePath != null) {
                    Path fileParent = filePath.toAbsolutePath().normalize().getParent();
                    dictSplit = Files.lines(filePath).mapToInt(l -> l.split("\t").length).findFirst().getAsInt();
                    dictFile = filePath;
                    fNames = Files.lines(filePath).map(l -> l.split("\t")).peek(l -> l[0] = l[0].split("\\|")[0]).collect(Collectors.toMap(s -> fileParent.resolve(s[0]), s -> s[1], (a1, a2) -> a1));
                    fileName = fNames.keySet().iterator().next().toString();
                    filePath = standalone != null && standalone.length() > 0 ? Paths.get(standalone).resolve(fileName) : Paths.get(fileName);
                    uNames = new HashMap<>();
                    for (Path p : fNames.keySet()) {
                        uNames.put(p.toUri().toString(), fNames.get(p));
                    }
                } else if (fns.length > 1) {
                    fNames = Arrays.stream(fns).collect(Collectors.toMap(Paths::get, s -> s));
                    fileName = fNames.keySet().iterator().next().toString();
                    filePath = standalone != null && standalone.length() > 0 ? Paths.get(standalone).resolve(fileName) : Paths.get(fileName);
                    uNames = new HashMap<>();
                    for (Path p : fNames.keySet()) {
                        uNames.put(p.toUri().toString(), fNames.get(p));
                    }
                } else {
                    fNames = null;
                    uNames = null;
                }

                if (fileName.startsWith("spark ")) {
                    PipeInstance pi = new PipeInstance(gorSparkSession.getGorContext());
                    PipeOptions po = new PipeOptions();
                    po.query_$eq(fileName);
                    pi.subProcessArguments(po);
                    //SparkRowSource sparkRowSource = new SparkRowSource(sparkSql, null, null, false, GorSparkSession, null, null, null, null, null, -1, -1, false, jobid, false);
                    SparkRowSource sparkRowSource = (SparkRowSource) pi.theInputSource();
                    gor = sparkRowSource.getDataset();
                    dataTypes = Arrays.stream(gor.schema().fields()).map(StructField::dataType).toArray(DataType[]::new);
                    //gor = registerFile();
                } else if (fileName.startsWith("pgor ") || fileName.startsWith("partgor ") || fileName.startsWith("parallel ") || fileName.startsWith("gor ") || fileName.startsWith("nor ") || fileName.startsWith("gorrows ") || fileName.startsWith("norrows ")) {
                    DataFrameReader dfr = gorSparkSession.getSparkSession().read().format(gordatasourceClassname);
                    dfr.option("query", fileName);
                    if (tag) dfr.option("tag", true);
                    dfr.option("projectroot", fileroot.toString());
                    dfr.option("cachedir", cacheDir.toString());
                    dfr.option("aliasfile", gorSparkSession.getProjectContext().getGorAliasFile());
                    dfr.option("configfile", gorSparkSession.getProjectContext().getGorConfigFile());
                    if(schema!=null) dfr.schema(schema);
                    gor = dfr.load();
                    dataTypes = Arrays.stream(gor.schema().fields()).map(StructField::dataType).toArray(DataType[]::new);
                } else if (fileName.toLowerCase().endsWith(".parquet")) {
                    gor = gorSparkSession.getSparkSession().read().format("org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2").load(fileName);
                    dataTypes = Arrays.stream(gor.schema().fields()).map(StructField::dataType).toArray(DataType[]::new);
                } else if (fileName.toLowerCase().endsWith(".vcf") || fileName.toLowerCase().endsWith(".vcf.gz") || fileName.toLowerCase().endsWith(".vcf.bgz")) {
                    //io.projectglow.vcf.VCFFileFormat f;
                    String vcfDataSource = "io.projectglow.vcf.VCFFileFormat"; //vcf
                    gor = gorSparkSession.getSparkSession().read().format(vcfDataSource).load(fileName);
                    dataTypes = Arrays.stream(gor.schema().fields()).map(StructField::dataType).toArray(DataType[]::new);
                } else if (fileName.toLowerCase().endsWith(".bgen")) {
                    //io.projectglow.vcf.VCFFileFormat f;
                    String bgenDataSource = "io.projectglow.bgen.BgenFileFormat"; //vcf
                    gor = gorSparkSession.getSparkSession().read().format(bgenDataSource).load(fileName);
                    dataTypes = Arrays.stream(gor.schema().fields()).map(StructField::dataType).toArray(DataType[]::new);
                } else if (splitFile != null && (fileName.toLowerCase().endsWith(".gor") || fileName.toLowerCase().endsWith(".nor") || fileName.toLowerCase().endsWith(".tsv") || fileName.toLowerCase().endsWith(".csv"))) {
                    DataFrameReader dfr = gorSparkSession.getSparkSession().read();
                    //if(fileName.toLowerCase().startsWith("s3")) dfr = dfr.format("s3selectCSV");
                    //else
                        dfr = dfr.format("csv");
                    dfr = dfr.option("header",true).option("inferSchema",true);
                    if(!fileName.toLowerCase().endsWith(".csv")) dfr = dfr.option("delimiter","\t");
                    gor = dfr.load(fileName);
                    dataTypes = Arrays.stream(gor.schema().fields()).map(StructField::dataType).toArray(DataType[]::new);
                } else {
                    boolean isGorz = fileName.toLowerCase().endsWith(".gorz");
                    boolean isGorgz = fileName.toLowerCase().endsWith(".gor.gz") || fileName.toLowerCase().endsWith(".gor.bgz");
                    GorDataType gorDataType = inferDataTypes(filePath, fileName, isGorz, nor);
                    String[] headerArray = gorDataType.header;
                    dataTypeMap = gorDataType.dataTypeMap;

                    dataTypes = new DataType[headerArray.length];
                    int start = 0;
                    if (!nor && dataTypes.length > 1 && headerArray[0].equalsIgnoreCase("chrom")) {
                        dataTypes[0] = StringType;
                        dataTypes[1] = IntegerType;
                        start = 2;
                    }
                    for (int i = start; i < dataTypes.length; i++) {
                        dataTypes[i] = dataTypeMap.getOrDefault(i, StringType);
                    }

                    Collection<String> pns = filter != null && filter.length() > 0 ? new HashSet<>(Arrays.asList(filter.split(","))) : fNames != null ? fNames.values() : Collections.emptySet();

                    final StructField[] fields;
                    //StructType schema;
                    if ((isGorz && !gorDataType.base128) || dictFile != null) {
                        if(schema==null) {
                            if (dictFile != null) {
                                Stream<StructField> baseStream = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty()));
                                Stream.Builder<StructField> sb = Stream.<StructField>builder();
                                if (dictSplit == 2 && (filterColumn != null && filterColumn.length() > 0))
                                    sb.add(new StructField(filterColumn, StringType, true, Metadata.empty()));
                                if (splitFile != null && splitFile.length() > 0)
                                    sb.add(new StructField("tag", StringType, true, Metadata.empty()));
                                Stream<StructField> extra = sb.build();
                                fields = Stream.concat(baseStream, extra).toArray(StructField[]::new);
                            } else if (gorDataType.withStart) {
                                StructField[] tmpfields = {new StructField("Chrom", StringType, true, Metadata.empty()), new StructField("Start", IntegerType, true, Metadata.empty()), new StructField("Stop", IntegerType, true, Metadata.empty()), new StructField("data", StringType, true, Metadata.empty())};
                                fields = tmpfields;
                            } else {
                                StructField[] tmpfields = {new StructField("Chrom", StringType, true, Metadata.empty()), new StructField("Pos", IntegerType, true, Metadata.empty()), new StructField("data", StringType, true, Metadata.empty())}; //IntStream.range(0,header.length).mapToObj(i -> new StructField(header[i], dataTypes[i], true, Metadata.empty())).toArray(size -> new StructField[size]);
                                fields = tmpfields;
                            }
                            schema = new StructType(fields);
                        }
                        if (uNames != null) {
                            // hey SparkGorUtilities.getSparkSession(GorSparkSession).udf().register("get_pn", (UDF1<String, String>) uNames::get, DataTypes.StringType);
                            if (dictFile != null) {
                                DataFrameReader dfr = gorSparkSession.getSparkSession().read().format(gordatasourceClassname);
                                if (fileroot != null) dfr.option("projectroot", fileroot.toString());
                                dfr.option("aliasfile", gorSparkSession.getProjectContext().getGorAliasFile());
                                dfr.option("configfile", gorSparkSession.getProjectContext().getGorConfigFile());
                                if (filter != null) dfr = dfr.option("f", filter);
                                if (filterFile != null) dfr = dfr.option("ff", filterFile);
                                if (splitFile != null) dfr = dfr.option("split", splitFile);
                                if (filterColumn != null) dfr = dfr.option("s", filterColumn);
                                if (chr != null) {
                                    String seek = chr;
                                    if (pos > 0 || end != -1) {
                                        seek += ":" + pos;
                                        if (end != -1) seek += "-" + end;
                                    }
                                    dfr = dfr.option("p", seek);
                                }
                                gor = dfr.schema(schema).load(dictFile.toAbsolutePath().normalize().toString());
                                isGorz = false;
                            } else {
                                gor = gorSparkSession.getSparkSession().read().format(csvDataSource).option("header", "true").option("delimiter", "\t").schema(schema).load(fNames.entrySet().stream().filter(e -> pns.contains(e.getValue())).map(Map.Entry::getKey).map(Path::toString).toArray(String[]::new));//.selectExpr("*","get_pn(input_file_name()) as PN");
                            }
                        } else {
                            gor = gorSparkSession.getSparkSession().read().format(csvDataSource).option("header", "true").option("delimiter", "\t").schema(schema).load(fileName); //.replace("s3://","s3n://"));
                        }
                    } else {
                        if(schema==null) {
                            fields = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty())).toArray(StructField[]::new);
                            schema = new StructType(fields);
                        }
                        if (uNames != null && !gorDataType.base128) {
                            gor = gorSparkSession.getSparkSession().read().format(csvDataSource).option("header", "true").option("delimiter", "\t").schema(schema).load(fNames.entrySet().stream().filter(e -> pns.contains(e.getValue())).map(Map.Entry::getKey).map(Path::toString).toArray(String[]::new));
                            if (filter != null && filter.length() > 0) {
                                // hey SparkGorUtilities.getSparkSession(GorSparkSession).udf().register("get_pn", (UDF1<String, String>) uNames::get, DataTypes.StringType);
                                gor = gor.selectExpr("*", "get_pn(input_file_name()) as PN");
                            }
                        } else {
                            if (isGorgz || gorDataType.base128 || splitFile != null) {
                                DataFrameReader dfr = gorSparkSession.getSparkSession().read().format(gordatasourceClassname).schema(schema);
                                if (gorSparkSession.getRedisUri() != null && gorSparkSession.getRedisUri().length() > 0) {
                                    dfr = dfr.option("redis", gorSparkSession.getRedisUri())
                                            .option("jobid", jobid)
                                            .option("cachefile", cacheFile)
                                            .option("native", Boolean.toString(cpp));
                                }
                                if (splitFile != null) dfr = dfr.option("split", splitFile);
                                if (chr != null) {
                                    String seek = chr;
                                    if (pos > 0 || end != -1) {
                                        seek += ":" + pos;
                                        if (end != -1) seek += "-" + end;
                                    }
                                    dfr = dfr.option("p", seek);
                                }
                                gor = dfr.load(fileName);
                            } else {
                                Dataset<org.apache.spark.sql.Row> sgor = gorSparkSession.getSparkSession().read().format(csvDataSource).option("header", "true").option("delimiter", "\t").schema(schema).load(fileName);
                                if (filter != null && filter.length() > 0) {
                                    int filterColumnIndex = headerArray.length - 1;
                                    if (filterColumn != null) {
                                        OptionalInt oi = IntStream.range(0, headerArray.length).filter(i -> headerArray[i].equals(filterColumn)).findFirst();
                                        if (oi.isPresent()) {
                                            filterColumnIndex = oi.getAsInt();
                                        }
                                    }
                                    FilterFunction<org.apache.spark.sql.Row> ff = new PNFilterFunction(filter, filterColumnIndex);
                                    sgor = sgor.filter(ff);
                                }
                                gor = sgor;
                                //GorSparkRowInferFunction gorSparkRowInferFunction = new GorSparkRowInferFunction();
                                //ReduceFunction<? extends Row> rowReduceFunction = null;
                                //Row row = (Row) gor.limit(100).reduce((ReduceFunction<org.apache.spark.sql.Row>) gorSparkRowInferFunction);
                            }
                        }
                    }

                    if (!isGorgz && !gorDataType.base128) {
                        if (isGorz) {
                            if (chr != null) {
                                if (gorDataType.withStart && end != -1) {
                                    gor = ((Dataset<org.apache.spark.sql.Row>) gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) <= end && row.getInt(2) >= pos);
                                } else {
                                    gor = ((Dataset<org.apache.spark.sql.Row>) gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) >= pos);
                                }
                            }

                            StructField[] flds = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty())).toArray(StructField[]::new);
                            schema = new StructType(flds);

                            ExpressionEncoder<org.apache.spark.sql.Row> encoder = RowEncoder.apply(schema);

                            final boolean withStart = gorDataType.withStart;
                            gor = ((Dataset<org.apache.spark.sql.Row>) gor).<org.apache.spark.sql.Row>flatMap((FlatMapFunction<org.apache.spark.sql.Row, org.apache.spark.sql.Row>) row -> {
                                String zip = withStart ? row.getString(3) : row.getString(2);
                                char tp = zip.charAt(0);
                                final CompressionType compressionLibrary = (tp & 0x02) == 0 ? CompressionType.ZLIB : CompressionType.ZSTD;
                                String zipo = zip.substring(1);
                                byte[] bb;
                                try {
                                    bb = Base64.getDecoder().decode(zipo);
                                } catch (Exception e) {
                                    bb = ByteArray.to8Bit(zipo.getBytes());
                                }
                                Unzipper unzip = new Unzipper();
                                unzip.setType(compressionLibrary);
                                unzip.setRawInput(bb, 0, bb.length);
                                int unzipLen = unzip.decompress(unzipBuffer, 0, unzipBuffer.length);
                                ByteArrayInputStream bais = new ByteArrayInputStream(unzipBuffer, 0, unzipLen);
                                InputStreamReader isr = new InputStreamReader(bais);
                                BufferedReader br = new BufferedReader(isr);
                                return (nor ? br.lines().map(line -> {
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
                            }, encoder);
                            if (chr != null) {
                                gor = ((Dataset<org.apache.spark.sql.Row>) gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> {
                                    int p = row.getInt(1);
                                    return chr.equals(row.getString(0)) && p >= pos && (end == -1 || p <= end);
                                });
                            }
                        } else if (chr != null) {
                            if (end != -1) {
                                gor = ((Dataset<org.apache.spark.sql.Row>) gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) <= end && row.getInt(2) >= pos);
                            } else {
                                gor = ((Dataset<org.apache.spark.sql.Row>) gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) >= pos);
                            }
                        }
                    }
                }
            }
            if (name != null && !name.startsWith("#")) {
                gor.createOrReplaceTempView(name);
            }
            gor.createOrReplaceTempView(tempViewName);
            gorSparkSession.datasetMap().put(tempViewName, new RowDataType(gor, dataTypes));
        }
        return gor;
    }

    static byte[] unzipBuffer = new byte[1 << 17];

    public static GorDataType inferDataTypes(Path filePath, String fileName, boolean isGorz, boolean nor) throws IOException, DataFormatException {
        boolean isUrl = fileName.contains("://");
        InputStream is = null;
        if (isUrl) {
            SourceReference sr = new SourceReference(fileName);
            is = ((StreamSource) GorDriverFactory.fromConfig().getDataSource(sr)).open();
        } else if (Files.exists(filePath)) {
            is = Files.newInputStream(filePath);
        }

        String fileLow = filePath.getFileName().toString().toLowerCase();
        boolean isCompressed = fileLow.endsWith(".gz") || fileLow.endsWith(".bgz");
        if (isCompressed) is = new GZIPInputStream(is);

        Stream<String> linestream = Stream.empty();
        boolean withStart = false;
        String[] headerArray = {};
        boolean base128 = false;
        if (is != null) {
            StringBuilder headerstr = new java.lang.StringBuilder();
            int r = is.read();
            while (r != -1 && r != '\n') {
                headerstr.append((char) r);
                r = is.read();
            }
            String header = headerstr.toString();
            if (header.startsWith("#")) header = header.substring(1);
            headerArray = header.split("\t");

            if (isGorz) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                r = is.read();
                if (r != -1) {
                    while (r != '\t') r = is.read();
                    r = is.read();
                    while (r != '\t') r = is.read();
                    r = is.read();
                    if (r >= '0' && r <= '9') {
                        withStart = true;
                        while (r != '\t') r = is.read();
                        is.read();
                    }
                    //r = is.read();
                    final CompressionType compressionLibrary = (r & 0x02) == 0 ? CompressionType.ZLIB : CompressionType.ZSTD;
                    r = is.read();
                    while (r != '\n') {
                        baos.write(r);
                        r = is.read();
                    }
                    is.close();
                    byte[] baosArray = baos.toByteArray();
                    byte[] bb;
                    try {
                        bb = Base64.getDecoder().decode(baosArray);
                    } catch (Throwable e) {
                        base128 = true;
                        bb = ByteArray.to8Bit(baosArray);
                    }
                    Unzipper unzip = new Unzipper();
                    unzip.setType(compressionLibrary);
                    unzip.setRawInput(bb, 0, bb.length);
                    int unzipLen = unzip.decompress(unzipBuffer, 0, unzipBuffer.length);
                    String str = new String(unzipBuffer, 0, unzipLen);
                    StringReader strreader = new StringReader(str);
                    linestream = new BufferedReader(strreader).lines();
                } else linestream = Stream.empty();
            } else {
                is.close();
                if (isUrl) {
                    SourceReference sr = new SourceReference(fileName);
                    is = ((StreamSource) GorDriverFactory.fromConfig().getDataSource(sr)).open();
                    if (isCompressed) is = new GZIPInputStream(is);
                    linestream = new BufferedReader(new InputStreamReader(is)).lines().skip(1);
                } else if (Files.exists(filePath)) {
                    linestream = isCompressed ? new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(filePath)))).lines().skip(1) : Files.newBufferedReader(filePath).lines().skip(1);
                }
            }
        }
        return typeFromStream(linestream, withStart, headerArray, nor, base128);
    }

    public static GorDataType typeFromParquetLine(ParquetLine pl, boolean withStart, String[] header) {
        Map<Integer, DataType> dataTypeMap = new HashMap<>();
        String[] gortypes = new String[pl.numCols()];
        for (int i = 0; i < pl.numCols(); i++) {
            PrimitiveType.PrimitiveTypeName ptm = pl.getType(i);
            if (ptm == PrimitiveType.PrimitiveTypeName.INT64) {
                dataTypeMap.put(i, DataTypes.LongType);
                gortypes[i] = "L";
            } else if (ptm == PrimitiveType.PrimitiveTypeName.INT32) {
                dataTypeMap.put(i, DataTypes.IntegerType);
                gortypes[i] = "I";
            } else if (ptm == PrimitiveType.PrimitiveTypeName.FLOAT) {
                dataTypeMap.put(i, DataTypes.FloatType);
                gortypes[i] = "D";
            } else {
                dataTypeMap.put(i, StringType);
                gortypes[i] = "S";
            }
        }
        return new GorDataType(dataTypeMap, withStart, header, gortypes);
    }

    public static GorDataType typeFromStream(Stream<String> linestream, boolean withStart, String[] headerArray, final boolean nor) {
        return typeFromStream(linestream, withStart, headerArray, nor, false);
    }

    public static GorDataType typeFromStream(Stream<String> linestream, boolean withStart, String[] headerArray, final boolean nor, boolean base128) {
        Map<Integer, DataType> dataTypeMap = new HashMap<>();
        if (nor) headerArray = Arrays.copyOfRange(headerArray, 2, headerArray.length);
        String[] gortypes = new String[headerArray.length];
        int start = 0;
        /*if (!nor && gortypes.length > 0) {
            gortypes[0] = "S";
            start = 1;
        }*/
        for (int i = start; i < headerArray.length; i++) {
            dataTypeMap.put(i, IntegerType);
            gortypes[i] = "I";
        }

        Set<Integer> remSet = new HashSet<>();
        Set<Integer> dSet = new HashSet<>();
        Stream<String[]> strstr = linestream.limit(1000).map(line -> line.split("\t", -1));
        if (nor) strstr = strstr.map(a -> Arrays.copyOfRange(a, 2, a.length));
        List<String[]> ok = strstr.collect(Collectors.toList());
        strstr = ok.stream();
        strstr.allMatch(line -> {
            dataTypeMap.forEach((idx, colType) -> {
                String value = line[idx];
                if (colType == IntegerType) {
                    try {
                        Integer.parseInt(value);
                    } catch (Exception e1) {
                        colType = DoubleType;
                    }
                }
                if (colType == DoubleType) {
                    try {
                        int di = value.indexOf('.');
                        if (di >= 0 || value.length() <= 16) {
                            Double.parseDouble(value);
                            dSet.add(idx);
                        } else {
                            remSet.add(idx);
                            //colType = doubleArrayType;
                        }
                    } catch (Exception e1) {
                        remSet.add(idx);
                        //colType = doubleArrayType;
                    }
                }
                /*if (colType == doubleArrayType) {
                    String val = line[idx];
                    String[] spl = val.split(",");
                    if(listLen[0] == -1 || spl.length == listLen[0]) {
                        listLen[0] = spl.length;
                        aSet.add(idx);
                    } else remSet.add(idx);
                }*/
            });
            if (remSet.size() > 0) {
                dataTypeMap.keySet().removeAll(remSet);
                for (int i : remSet) gortypes[i] = "S";
                remSet.clear();
            }
            for (int i : dSet) {
                dataTypeMap.put(i, DoubleType);
                gortypes[i] = "D";
            }
            return dataTypeMap.size() > 0;
        });

        return new GorDataType(dataTypeMap, withStart, headerArray, gortypes, base128);
    }
}
