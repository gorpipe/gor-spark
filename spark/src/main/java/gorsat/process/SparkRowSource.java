package gorsat.process;

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

import org.gorpipe.gor.ProjectContext;
import org.gorpipe.spark.*;
import gorsat.Commands.Analysis;
import gorsat.Commands.CommandParseUtilities;
import gorsat.DynIterator;
import gorsat.RowBuffer;
import gorsat.parser.ParseArith;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import io.projectglow.Glow;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import org.gorpipe.gor.GorSession;
import org.gorpipe.gor.driver.GorDriverFactory;
import org.gorpipe.gor.driver.meta.SourceReference;
import org.gorpipe.gor.driver.providers.stream.sources.StreamSource;
import org.gorpipe.model.genome.files.binsearch.CompressionType;
import org.gorpipe.model.genome.files.binsearch.Unzipper;
import org.gorpipe.model.genome.files.gor.*;
import org.gorpipe.model.genome.files.gor.Row;
import org.gorpipe.util.collection.ByteArray;
import scala.Function1;
import scala.collection.JavaConverters;


import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Created by sigmar on 12/02/16.
 */
public class SparkRowSource extends ProcessSource {
    static final String csvDataSource = "csv";
    static final String gordatasourceClassname = "gorsat.spark.GorDataSource";

    static class FilterParams implements Serializable {
        FilterParams(String paramString, String[] headersplit, String[] colType) {
            this.paramString = paramString;
            this.headersplit = headersplit;
            this.colType = colType;
        }

        public String paramString;
        String[] headersplit;
        String[] colType;
    }

    String errorStr = "";
    List<String> commands;
    String type;
    boolean isGorRow = false;
    Dataset<? extends org.apache.spark.sql.Row> dataset;
    Iterator<Row> it;
    boolean nor;
    ProcessBuilder pb;
    Process p;
    Path fileroot = null;
    String parquetPath = null;
    String pushdownGorPipe = null;
    GorSparkSession gorSparkSession;

    static Map<String, DataType> tmap = new HashMap<>();
    Map<DataType, String> dmap = new HashMap<>();
    Map<String, DataType> dsmap = new HashMap<>();

    String chr;
    int start;
    int end;

    String jobId = "-1";

    Integer buckets;
    String parts;
    boolean tag;

    public static class GorDataType {
        public Map<Integer, DataType> dataTypeMap;
        public boolean withStart;
        public String[] header;
        public String[] gortypes;
        boolean base128;
        public List<String> usedFiles;

        public GorDataType(Map<Integer, DataType> dataTypeMap, boolean withStart, String[] header, String[] gortypes, boolean base128) {
            this.dataTypeMap = dataTypeMap;
            this.withStart = withStart;
            this.header = header;
            this.gortypes = gortypes;
            this.base128 = base128;
        }

        public GorDataType(Map<Integer, DataType> dataTypeMap, boolean withStart, String[] header, String[] gortypes) {
            this(dataTypeMap, withStart, header, gortypes, false);
        }

        public void setUsedFiles(List<String> usedFiles) {
            this.usedFiles = usedFiles;
        }
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
        if(isCompressed) is = new GZIPInputStream(is);

        Stream<String> linestream = Stream.empty();
        boolean withStart = false;
        String[] headerArray = {};
        boolean base128 = false;
        if (is != null) {
            StringBuilder headerstr = new java.lang.StringBuilder();
            int r = is.read();
            while (r != '\n') {
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
                    if(isCompressed) is = new GZIPInputStream(is);
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
        for( int i = 0; i < pl.numCols(); i++ ) {
            PrimitiveType.PrimitiveTypeName ptm = pl.getType(i);
            if( ptm == PrimitiveType.PrimitiveTypeName.INT64 ) {
                dataTypeMap.put(i, DataTypes.LongType);
                gortypes[i] = "L";
            } else if( ptm == PrimitiveType.PrimitiveTypeName.INT32 ) {
                dataTypeMap.put(i, DataTypes.IntegerType);
                gortypes[i] = "I";
            } else if( ptm == PrimitiveType.PrimitiveTypeName.FLOAT ) {
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
        if(nor) headerArray = Arrays.copyOfRange(headerArray,2,headerArray.length);
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
        Stream<String[]> strstr = linestream.limit(1000).map(line -> line.split("\t"));
        if (nor) strstr = strstr.map(a -> Arrays.copyOfRange(a, 2, a.length));
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

    static class PNFilterFunction implements FilterFunction<org.apache.spark.sql.Row>, Serializable {
        Set<String> pns;
        int colnum;

        public PNFilterFunction(String filter, int colNum) {
            pns = new HashSet<>(Arrays.asList(filter.split(",")));
            colnum = colNum;
        }

        @Override
        public boolean call(org.apache.spark.sql.Row row) {
            String str = row.getString(colnum);
            return pns.contains(str);
        }
    }

    static class GorMapFunction implements MapFunction<org.apache.spark.sql.Row, org.apache.spark.sql.Row>, Serializable {
        transient ParseArith filter;
        String calcType;
        Function1 func;
        int replaceIndex;

        GorMapFunction(FilterParams filterParams, OptionalInt rIdx) {
            filter = new ParseArith(null);
            filter.setColumnNamesAndTypes(filterParams.headersplit, filterParams.colType);
            calcType = filter.compileCalculation(filterParams.paramString);
            replaceIndex = rIdx.isPresent() ? rIdx.getAsInt() : -1;

            if (calcType.equals("String")) func = filter.stringFunction();
            else if (calcType.equals("Double")) func = filter.doubleFunction();
            else if (calcType.equals("Long")) func = filter.longFunction();
            else if (calcType.equals("Int")) func = filter.intFunction();
            else if (calcType.equals("Boolean")) func = filter.booleanFunction();
        }

        public String getCalcType() {
            return calcType;
        }

        @Override
        public org.apache.spark.sql.Row call(org.apache.spark.sql.Row row) {
            Object[] lobj = replaceIndex == -1 ? new Object[row.size() + 1] : new Object[row.size()];
            for (int i = 0; i < row.size(); i++) {
                lobj[i] = row.get(i);
            }
            GorSparkRow cvp = new GorSparkRow(row);
            lobj[replaceIndex == -1 ? row.size() : replaceIndex] = func != null ? func.apply(cvp) : "";
            return org.apache.spark.sql.RowFactory.create(lobj);
        }
    }

    static class NorMapFunction extends GorMapFunction {
        NorMapFunction(FilterParams filterParams, OptionalInt replaceIndex) {
            super(filterParams, replaceIndex);
        }

        @Override
        public org.apache.spark.sql.Row call(org.apache.spark.sql.Row row) {
            Object[] lobj = replaceIndex == -1 ? new Object[row.size() + 1] : new Object[row.size()];
            for (int i = 0; i < row.size(); i++) {
                lobj[i] = row.get(i);
            }
            SparkRow cvp = new SparkRow(row);
            lobj[replaceIndex == -1 ? row.size() : replaceIndex] = func != null ? func.apply(cvp) : "";
            return org.apache.spark.sql.RowFactory.create(lobj);
        }
    }

    java.util.function.Function<String, String> inner;
    java.util.function.Function<String, String> gorfunc;
    java.util.function.Predicate<String> gorpred;
    java.util.function.Function<String, String> parqfunc;
    java.util.function.Function<String, Stream<String>> gorfileflat;

    public Dataset<? extends org.apache.spark.sql.Row> getDataset() {
        return dataset;
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
            if(!filePath.isAbsolute()) {
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

    public static Dataset<? extends org.apache.spark.sql.Row> registerFile(String[] fns, String name, GorSparkSession gorSparkSession, String standalone, Path fileroot, boolean usestreaming, String filter, String filterFile, String filterColumn, String splitFile, final boolean nor, final String chr, final int pos, final int end, final String jobid, String cacheFile, boolean cpp, boolean tag) throws IOException, DataFormatException {
        String fn = fns[0];
        boolean nestedQuery = fn.startsWith("<(");
        Path filePath = null;
        String fileName;
        if(nestedQuery) {
            fileName = fn.substring(2, fn.length()-1);
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
                        headerArray = Arrays.copyOf(gdt.header,gdt.header.length+1);
                        headerArray[headerArray.length-1] = "PN";
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
                StructType schema = new StructType(fields);

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
                if (isGord) {
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
                    fNames = Arrays.stream(fns).collect(Collectors.toMap(s -> Paths.get(s), s -> s));
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
                    SparkRowSource sparkRowSource = (SparkRowSource)pi.theInputSource();
                    gor = sparkRowSource.getDataset();
                    dataTypes = Arrays.stream(gor.schema().fields()).map(StructField::dataType).toArray(DataType[]::new);
                    //gor = registerFile();
                } else if (fileName.startsWith("pgor ") || fileName.startsWith("partgor ") || fileName.startsWith("parallel ") || fileName.startsWith("gor ") || fileName.startsWith("nor ")) {
                    DataFrameReader dfr = gorSparkSession.getSparkSession().read().format(gordatasourceClassname);
                    dfr.option("query",fileName);
                    if(tag) dfr.option("tag",true);
                    dfr.option("projectroot",fileroot.toString());
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
                    StructType schema;
                    if ((isGorz && !gorDataType.base128) || dictFile != null) {
                        if (dictFile != null) {
                            Stream<StructField> baseStream = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty()));
                            Stream.Builder<StructField> sb = Stream.<StructField>builder();
                            if (dictSplit == 2 && (filterColumn != null && filterColumn.length() > 0)) sb.add(new StructField(filterColumn, StringType, true, Metadata.empty()));
                            if (splitFile != null && splitFile.length() > 0) sb.add(new StructField("tag", StringType, true, Metadata.empty()));
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
                        if (uNames != null) {
                            // hey SparkGorUtilities.getSparkSession(GorSparkSession).udf().register("get_pn", (UDF1<String, String>) uNames::get, DataTypes.StringType);
                            if (dictFile != null) {
                                DataFrameReader dfr = gorSparkSession.getSparkSession().read().format(gordatasourceClassname);
                                dfr.option("projectroot",fileroot.toString());
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
                        fields = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty())).toArray(StructField[]::new);
                        schema = new StructType(fields);
                        if (uNames != null && !gorDataType.base128) {
                            gor = gorSparkSession.getSparkSession().read().format(csvDataSource).option("header", "true").option("delimiter", "\t").schema(schema).load(fNames.entrySet().stream().filter(e -> pns.contains(e.getValue())).map(Map.Entry::getKey).map(Path::toString).toArray(String[]::new));
                            if (filter != null && filter.length() > 0) {
                                // hey SparkGorUtilities.getSparkSession(GorSparkSession).udf().register("get_pn", (UDF1<String, String>) uNames::get, DataTypes.StringType);
                                gor = gor.selectExpr("*", "get_pn(input_file_name()) as PN");
                            }
                        } else {
                            if (isGorgz || gorDataType.base128) {
                                DataFrameReader dfr = gorSparkSession.getSparkSession().read().format(gordatasourceClassname).schema(schema);
                                if (gorSparkSession.getRedisUri() != null && gorSparkSession.getRedisUri().length() > 0) {
                                    dfr = dfr.option("redis", gorSparkSession.getRedisUri()).option("jobid", jobid).option("cachefile", cacheFile).option("native", Boolean.toString(cpp));
                                }
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
                                    gor = ((Dataset<org.apache.spark.sql.Row>)gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) <= end && row.getInt(2) >= pos);
                                } else {
                                    gor = ((Dataset<org.apache.spark.sql.Row>)gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) >= pos);
                                }
                            }

                            StructField[] flds = IntStream.range(0, headerArray.length).mapToObj(i -> new StructField(headerArray[i], dataTypes[i], true, Metadata.empty())).toArray(StructField[]::new);
                            schema = new StructType(flds);
                            ExpressionEncoder<org.apache.spark.sql.Row> encoder = RowEncoder.apply(schema);

                            final boolean withStart = gorDataType.withStart;
                            gor = ((Dataset<org.apache.spark.sql.Row>)gor).<org.apache.spark.sql.Row>flatMap((FlatMapFunction<org.apache.spark.sql.Row, org.apache.spark.sql.Row>) row -> {
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
                                gor = ((Dataset<org.apache.spark.sql.Row>)gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> {
                                    int p = row.getInt(1);
                                    return chr.equals(row.getString(0)) && p >= pos && (end == -1 || p <= end);
                                });
                            }
                        } else if (chr != null) {
                            if (end != -1) {
                                gor = ((Dataset<org.apache.spark.sql.Row>)gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) <= end && row.getInt(2) >= pos);
                            } else {
                                gor = ((Dataset<org.apache.spark.sql.Row>)gor).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) >= pos);
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

    static {
        tmap.put("S", StringType);
        tmap.put("I", IntegerType);
        tmap.put("D", DoubleType);
    }

    public void init() {
        dmap.put(StringType, "S");
        dmap.put(IntegerType, "I");
        dmap.put(DoubleType, "D");

        dsmap.put("String", StringType);
        dsmap.put("Integer", IntegerType);
        dsmap.put("Int", IntegerType);
        dsmap.put("Double", DoubleType);
    }

    public boolean isNor() {
        return nor;
    }

    public SparkRowSource(String sql, String parquet, String type, boolean nor, GorSparkSession gpSession, final String filter, final String filterFile, final String filterColumn, final String splitFile, final String chr, final int pos, final int end, boolean usestreaming, String jobId, boolean useCpp, String parts, int buckets, boolean tag) throws IOException, DataFormatException {
        init();
        this.jobId = jobId;
        this.tag = tag;

        this.buckets = buckets != -1 ? buckets : null;
        this.parts = parts;

        this.gorSparkSession = gpSession;
        this.nor = nor;
        if (parquet != null && Files.exists(Paths.get(parquet))) {
            dataset = gpSession.getSparkSession().read().parquet(parquet);
        } else {
            this.type = type;
            commands = new ArrayList<>();

            this.chr = chr;
            this.start = pos;
            this.end = end;

            String root = gpSession.getProjectContext().getRoot();
            if (root != null && root.length() > 0) {
                int i = root.indexOf(' ');
                if (i == -1) i = root.length();
                fileroot = Paths.get(root.substring(0, i));
            }

            String[] cmdsplit = CommandParseUtilities.quoteCurlyBracketsSafeSplit(sql, ' ');
            commands.addAll(Arrays.asList(cmdsplit));

            boolean bamvcf = type != null && (type.equals("bam") || type.equals("sam") || type.equals("cram") || type.equals("vcf"));
            List<String> headercommands = bamvcf ? seekCmd(null, 0, -1) : seekCmd(chr, start, end);

            String standalone = System.getProperty("sm.standalone");

            inner = p -> {
                if (p.startsWith("(")) {
                    String[] cmdspl = CommandParseUtilities.quoteCurlyBracketsSafeSplit(p.substring(1, p.length() - 1), ' ');
                    return Arrays.stream(cmdspl).map(inner).map(gorfunc).map(parqfunc).collect(Collectors.joining(" ", "(", ")"));
                } else return p;
            };
            gorpred = p -> p.toLowerCase().endsWith(".tsv") || p.toLowerCase().endsWith(".gor") || p.toLowerCase().endsWith(".gorz") || p.toLowerCase().endsWith(".gor.gz") || p.toLowerCase().endsWith(".gord") || p.toLowerCase().endsWith(".txt") || p.toLowerCase().endsWith(".vcf") || p.toLowerCase().endsWith(".bgen") || p.startsWith("<(");
            gorfunc = p -> {
                if (gorpred.test(p)) {
                    boolean nestedQuery = p.startsWith("<(");
                    String fileName;
                    if(nestedQuery) {
                        fileName = p.substring(2,p.length()-1);
                    } else {
                        fileName = translatePath(p, fileroot, standalone);
                    }
                    return generateTempViewName(fileName, usestreaming, filter, chr, pos, end);
                }
                return p;
            };
            gorfileflat = p -> p.startsWith("(") ? Arrays.stream(CommandParseUtilities.quoteCurlyBracketsSafeSplit(p.substring(1, p.length() - 1), ' ')).flatMap(gorfileflat).filter(gorpred) : Stream.of(p);
            parqfunc = p -> {
                if (p.toLowerCase().endsWith(".parquet") && !p.toLowerCase().startsWith("parquet.")) {
                    String fileName = translatePath(p, fileroot, standalone);
                    return "parquet.`" + fileName + "`";
                } else return p;
            };

            boolean isSql = headercommands.get(0).equalsIgnoreCase("select");
            String[] fileNames;
            String cacheFile = null;
            if (isSql) {
                sql = headercommands.stream().filter(p -> p.length() > 0).map(inner).map(gorfunc).map(parqfunc).collect(Collectors.joining(" "));
                fileNames = Arrays.stream(cmdsplit).flatMap(gorfileflat).filter(gorpred).toArray(String[]::new);
                for (String fn : fileNames) {
                    if(gorSparkSession.getSystemContext().getServer()) ProjectContext.validateServerFileName(fn, true);
                    registerFile(new String[]{fn}, null, gpSession, standalone, fileroot, usestreaming, filter, filterFile, filterColumn, splitFile, nor, chr, pos, end, jobId, cacheFile, useCpp, tag);
                }
                dataset = gpSession.getSparkSession().sql(sql);
            } else {
                fileNames = headercommands.toArray(new String[0]);
                dataset = registerFile(fileNames, null, gpSession, standalone, fileroot, usestreaming, filter, filterFile, filterColumn, splitFile, nor, chr, pos, end, jobId, cacheFile, useCpp, tag);
            }

            if (chr != null) {
                if (end != -1) {
                    dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) <= end && row.getInt(1) >= pos);
                } else {
                    dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) >= pos);
                }
            }
            gpSession.getSparkSession().sparkContext().setJobGroup("a|b|gorsql|c", sql, true);
        }
        setHeader((nor ? "chrNOR\tposNOR\t" : "") + correctHeader(dataset.columns()));
    }

    private String correctHeader(String[] header) {
        return String.join("\t", header);
    }

    public void gorpipe(Analysis pipeStep, boolean gor) {
        RDD rdd = dataset.rdd();
        ExpressionEncoder encoder = dataset.exprEnc();
        GorpipeRDD<org.apache.spark.sql.Row> gorpipeRDD = new GorpipeRDD<org.apache.spark.sql.Row>(rdd, pipeStep, encoder, getHeader(), gor, rdd.elementClassTag());
        dataset = gorSparkSession.getSparkSession().createDataset(gorpipeRDD, encoder);
        setHeader(correctHeader(dataset.columns()));
    }

    public void gor() {
        String inputHeader = super.getHeader();

        boolean nor = checkNor(dataset.schema().fields());
        Dataset<? extends Row> dr = checkRowFormat(dataset);

        String uri = gorSparkSession.getRedisUri();
        GorSpark gs = new GorSparkMaterialize(inputHeader, nor, SparkGOR.sparkrowEncoder().schema(), pushdownGorPipe, gorSparkSession.getProjectContext().getRoot(), uri, jobId, 100);
        GorSparkRowInferFunction gi = new GorSparkRowInferFunction();
        Row row = ((Dataset<Row>) dr).mapPartitions(gs, SparkGOR.gorrowEncoder()).limit(100).reduce(gi);
        if (row.chr != null) row = gi.infer(row, row);
        StructType schema = schemaFromRow(gs.query().getHeader().split("\t"), row);

        this.setHeader(correctHeader(schema.fieldNames()));
        ExpressionEncoder encoder = RowEncoder.apply(schema);
        gs = new GorSpark(inputHeader, nor, schema, pushdownGorPipe, gorSparkSession.getProjectContext().getRoot(), uri, jobId);
        pushdownGorPipe = null;
        dataset = ((Dataset<Row>) dr).mapPartitions(gs, encoder);

        nor = checkNor(dataset.schema().fields());
        setHeader((nor ? "chrNOR\tposNOR\t" : "") + correctHeader(dataset.columns()));

        //GorSparkSession.getSparkSession().sparkContext().setJobGroup("group gor", pushdownGorPipe, true);
    }

    public static StructType schemaFromRow(String[] header, Row row) {
        return new StructType(IntStream.range(0, row.numCols()).mapToObj(i -> new StructField(header[i], tmap.get(row.stringValue(i)), true, Metadata.empty())).toArray(StructField[]::new));
    }

    public static String checkNested(String cmd, GorSession gpSession, String[] errorStr) {
        String ncmd;
        if (cmd.startsWith("<(")) {
            String tmpdir = System.getProperty("java.io.tmpdir");
            if (tmpdir == null || tmpdir.length() == 0) tmpdir = "/tmp";
            Path tmpath = Paths.get(tmpdir);
            String scmd = cmd.substring(2, cmd.length() - 1);
            Path fifopath = tmpath.resolve(Integer.toString(Math.abs(scmd.hashCode())));
            String pipename = fifopath.toAbsolutePath().toString();
            DynIterator.DynamicRowSource drs = new DynIterator.DynamicRowSource(scmd, gpSession.getGorContext(), false);
            try {
                if (!Files.exists(fifopath)) {
                    ProcessBuilder mkfifo = new ProcessBuilder("mkfifo", pipename);
                    Process p = mkfifo.start();
                    p.waitFor();
                }
                Thread t = new Thread(() -> {
                    try (OutputStream os = Files.newOutputStream(fifopath)) {
                        os.write(String.join("\t", drs.getHeader()).getBytes());
                        os.write('\n');
                        while (drs.hasNext()) {
                            String rowstr = drs.next().toString();
                            os.write(rowstr.getBytes());
                            os.write('\n');
                        }
                    } catch (IOException e) {
                        errorStr[0] += e.getMessage();
                    } finally {
                        try {
                            Files.delete(fifopath);
                        } catch (IOException e) {
                            // Ignore
                        }
                    }
                });
                t.start();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException("Failed starting fifo thread", e);
            }
            ncmd = pipename;
        } else {
            boolean quotas = cmd.startsWith("'") || cmd.startsWith("\"");
            ncmd = quotas ? cmd.substring(1, cmd.length() - 1) : cmd;
            if (quotas) ncmd = ncmd.replace("\\t", "\t").replace("\\n", "\n");
        }
        return ncmd;
    }

    public SparkRowSource(String[] cmds, String type, boolean nor, GorSession gpSession, String chr, int pos, int end, int bs) {
        this.type = type;
        this.nor = nor;
        this.setBufferSize(bs);
        commands = new ArrayList<>();

        this.chr = chr;
        this.start = pos;
        this.end = end;

        if (gpSession != null) {
            String root = gpSession.getProjectContext().getRoot();
            if (root != null && root.length() > 0) {
                int i = root.indexOf(' ');
                if (i == -1) i = root.length();
                fileroot = Paths.get(root.substring(0, i));
            }
        }

        String[] estr = {errorStr};
        for (String cmd : cmds) {
            String ncmd = checkNested(cmd, gpSession, estr);
            commands.add(ncmd);
        }

        boolean bamvcf = type != null && (type.equals("bam") || type.equals("sam") || type.equals("cram") || type.equals("vcf"));
        List<String> headercommands = bamvcf ? seekCmd(null, 0, -1) : seekCmd(chr, start, end);

        try {
            List<String> rcmd = headercommands.stream().filter(p -> p.length() > 0).collect(Collectors.toList());
            pb = new ProcessBuilder(rcmd);
            if (fileroot != null) pb.directory(fileroot.toFile());
            p = pb.start();
            Thread errorThread = new Thread(() -> {
                try {
                    StringBuilder total = new StringBuilder();
                    InputStream es = p.getErrorStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(es));
                    String line = br.readLine();
                    while (line != null) {
                        total.append(line).append("\n");
                        line = br.readLine();
                    }
                    errorStr += total.toString();
                    br.close();
                } catch (IOException e) {
                    // don't care throw new RuntimeException("", e);
                }
            });
            errorThread.start();
            InputStream is = p.getInputStream();

            if (type == null || type.equalsIgnoreCase("gor")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                setHeader(br.readLine());
                if (getHeader() == null) {
                    throw new RuntimeException("Running external process: " + String.join(" ", headercommands) + " with error: " + errorStr);
                }
                if (nor) setHeader("ChromNOR\tPosNOR\t" + getHeader().replace(" ", "_").replace(":", ""));
            } else if (type.equalsIgnoreCase("vcf")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                GenomicIterator.ChromoLookup lookup = ProcessRowSource.createChromoLookup();
                try {
                    it = new VcfGzGenomicIterator(lookup, "filename", null, br) {
                        @Override
                        public boolean seek(String seekChr, int seekPos) {
                            return seek(seekChr, seekPos, lookup.chrToLen(seekChr));
                        }

                        @Override
                        public boolean seek(String seekChr, int seekPos, int endPos) {
                            try {
                                reader.close();
                                if (seekChr != null && this.chrNameSystem != VcfGzTabixGenomicIterator.ChrNameSystem.WITH_CHR_PREFIX)
                                    seekChr = seekChr.substring(3);
                                InputStream is1 = setRange(seekChr, seekPos, endPos);
                                reader = new BufferedReader(new InputStreamReader(is1));
                                next = reader.readLine();
                                while (next != null && next.startsWith("#")) {
                                    next = reader.readLine();
                                }
                            } catch (IOException e) {
                                throw new RuntimeException("Error reading next line from external process providing vcf stream", e);
                            }
                            return true;
                        }

                        @Override
                        public void close() {
                            super.close();
                        }
                    };
                } catch (Exception e) {
                    int exitValue = 0;
                    try {
                        exitValue = p.waitFor();
                    } catch (InterruptedException ignored) {
                    }
                    throw new RuntimeException("Error initializing vcf reader. Exit value from process: " + exitValue + ". Error from process: " + errorStr, e);
                }
            } else if (type.equalsIgnoreCase("bam") || type.equalsIgnoreCase("sam") || type.equalsIgnoreCase("cram")) {
                GenomicIterator.ChromoLookup lookup = ProcessRowSource.createChromoLookup();
                SamReaderFactory srf = SamReaderFactory.makeDefault().validationStringency(ValidationStringency.SILENT);
                SamInputResource sir = SamInputResource.of(is);
                SamReader samreader = srf.open(sir);
                BamIterator bamit = new BamIterator() {
                    @Override
                    public boolean seek(String chr, int pos) {
                        return seek(chr, pos);
                    }

                    @Override
                    public boolean seek(String chr, int pos, int end) {
                        int chrId = lookup.chrToId(chr); // Mark that a single chromosome seek
                        if (chrnamesystem == 1) { // BAM data on hg chromsome names, use the hg name for the chromsome for the seek
                            chr = ChromoCache.getHgName(chrId);
                        } else if (chrnamesystem == 2) {
                            chr = ChromoCache.getStdChrName(chrId);
                        }

                        try {
                            this.reader.close();
                        } catch (IOException e) {
                            // don't care if external process stream has already been closed
                        }
                        InputStream nis = setRange(chr, pos, end);
                        SamInputResource sir = SamInputResource.of(nis);
                        this.reader = srf.open(sir);
                        this.it = this.reader.iterator();
                        this.pos = pos;

                        return true;
                    }

                    @Override
                    public boolean hasNext() {
                        initIterator();
                        boolean hasNext = it.hasNext();
                        SAMRecord samRecord;
                        while (hasNext && (samRecord = it.next()) != null && (samRecord.getReadUnmappedFlag() || "*".equals(samRecord.getCigarString()) || samRecord.getStart() < pos)) {
                            hasNext = it.hasNext();
                        }
                        if (!hasNext) {
                            if (hgSeekIndex >= 0) { // Is seeking through differently ordered data
                                while (++hgSeekIndex < ChrDataScheme.ChrLexico.getOrder2id().length) {
                                    String name = getChromName();
                                    if (samFileHeader.getSequenceIndex(name) > -1) {
                                        createIterator(name, 0);
                                        return hasNext();
                                    }
                                }
                            }
                        }
                        return hasNext;
                    }

                    @Override
                    public void createIterator(String chr, int pos) {
                        if (it == null) it = reader.iterator();
                    }
                };
                bamit.init(lookup, samreader, null, false);
                bamit.it = bamit.reader.iterator();
            }
        } catch (IOException e) {
            throw new RuntimeException("unable to get header from process " + commands.get(0), e);
        }
    }

    public static boolean checkNor(StructField[] fields) {
        return fields.length == 1 || !(fields[0].name().equalsIgnoreCase("chrom") && fields[1].dataType() == IntegerType);
    }

    private RowBuffer rowBuffer = null;

    @Override
    public boolean hasNext() {
        if (it == null) {
            if (parquetPath != null) {
                Path pPath = Paths.get(parquetPath);
                if (fileroot != null && !pPath.isAbsolute()) {
                    pPath = fileroot.resolve(pPath);
                }
                if (!Files.exists(pPath)) {
                    Arrays.stream(dataset.columns()).filter(c -> c.contains("(")).forEach(c -> dataset = dataset.withColumnRenamed(c, c.replace('(', '_').replace(')', '_')));
                    DataFrameWriter dfw = dataset.write();
                    if(parts != null) {
                        if(buckets != null) {
                            dfw = dfw.bucketBy(buckets, parts);
                        } else {
                            dfw = dfw.partitionBy(parts.split(","));
                        }
                    }
                    dfw.format("parquet").mode(SaveMode.Overwrite).save(pPath.toAbsolutePath().normalize().toString());
                }
                return false;
            } else {
                Iterable<? extends org.apache.spark.sql.Row> iterable = () -> (Iterator<org.apache.spark.sql.Row>) dataset.toLocalIterator();
                boolean lng = false;
                if (dataset != null) {
                    StructField[] fields = dataset.schema().fields();
                    lng = fields.length > 1 && fields[1].dataType() == DataTypes.LongType;
                    nor = nor | checkNor(fields);
                }
                it = (nor ? StreamSupport.stream(iterable.spliterator(), false)
                        .map(r -> (Row) new SparkRow(r))
                        : lng ? StreamSupport.stream(iterable.spliterator(), false)
                        .map(r -> (Row) new LongGorSparkRow(r))
                        : StreamSupport.stream(iterable.spliterator(), false)
                        .map(r -> (Row) new GorSparkRow(r))
                ).iterator();
            }
        }
        return it.hasNext();
    }

    int linesRead = 0;

    @Override
    public Row next() {
        linesRead++;
        return it.next();
    }

    @Override
    public void setPosition(String seekChr, int seekPos) {

    }

    @Override
    public void close() {

    }

    private List<String> seekCmd(String seekChr, int startPos, int endPos) {
        List<String> seekcmd = new ArrayList<>();
        for (String cmd : commands) {
            if (seekChr == null) {
                int hPos = cmd.indexOf("#(H:");
                if (hPos != -1) {
                    int hEnd = cmd.indexOf(')', hPos + 1);
                    cmd = cmd.substring(0, hPos) + cmd.substring(hPos + 4, hEnd) + cmd.substring(hEnd + 1);
                }

                int sPos = cmd.indexOf("#(S:");
                if (sPos != -1) {
                    int sEnd = cmd.indexOf(')', sPos + 1);
                    cmd = cmd.substring(0, sPos) + cmd.substring(sEnd + 1);
                }
            } else {
                int hPos = cmd.indexOf("#(H:");
                if (hPos != -1) {
                    int hEnd = cmd.indexOf(')', hPos + 1);
                    cmd = cmd.substring(0, hPos) + cmd.substring(hEnd + 1);
                }

                int sPos = cmd.indexOf("#(S:");
                if (sPos != -1) {
                    int sEnd = cmd.indexOf(')', sPos + 1);

                    String seek = "";
                    seek = cmd.substring(sPos + 4, sEnd).replace("chr", seekChr);
                    if (seekChr.startsWith("chr")) seek = seek.replace("chn", seekChr.substring(3));
                    int pos = seek.indexOf("pos-end");
                    if (pos != -1) {
                        if (endPos == -1) {
                            int len = Integer.MAX_VALUE;
                            //if( it != null && it.getLookup() != null ) it.getLookup().chrToLen(seekChr);
                            seek = seek.replace("pos", (startPos + 1) + "").replace("end", len + "");
                        } else {
                            seek = seek.replace("pos", (startPos + 1) + "").replace("end", endPos + "");
                        }
                    } else if (seek.contains("pos")) {
                        seek = seek.replace("pos", startPos + "");
                        seek = seek.replace("end", endPos + "");
                    }
                    cmd = cmd.substring(0, sPos) + seek + cmd.substring(sEnd + 1);
                }
            }
            seekcmd.add(cmd);
        }
        return seekcmd;
    }

    @Override
    public InputStream setRange(String seekChr, int startPos, int endPos) {
        try {
            List<String> seekcmd = seekCmd(seekChr, startPos, endPos);
            if (p != null && p.isAlive()) {
                linesRead = 0;
                p.destroy();
            }
            pb = new ProcessBuilder(seekcmd.stream().filter(p -> p.length() > 0).collect(Collectors.toList()));
            if (fileroot != null) pb.directory(fileroot.toFile());
            p = pb.start();

            Thread errorThread = new Thread(() -> {
                try {
                    InputStream es = p.getErrorStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(es));
                    String line = br.readLine();
                    while (line != null) {
                        errorStr += line + "\n";
                        line = br.readLine();
                    }
                    br.close();
                } catch (IOException e) {
                    // don't care throw new RuntimeException("Error reading stderr from external process", e);
                }
            });
            errorThread.start();

            return p.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException("Unable to read line from external process in seek: " + commands, e);
        }
    }

    @Override
    public String getHeader() {
        if (pushdownGorPipe != null && pushdownGorPipe.length() > 0) gor();
        return super.getHeader();
    }

    @Override
    public boolean isBuffered() {
        return true;
    }

    public Dataset<? extends Row> checkRowFormat(Dataset<? extends org.apache.spark.sql.Row> dataset) {
        Dataset<? extends Row> ret;
        if (!isGorRow) {
            isGorRow = true;
            StructField[] fields = dataset.schema().fields();
            boolean lng = fields.length > 1 && fields[1].dataType() == DataTypes.LongType;
            nor = nor | checkNor(fields);
            Dataset<org.apache.spark.sql.Row> dr = (Dataset<org.apache.spark.sql.Row>) dataset;
            if (nor) {
                ret = dr.map((MapFunction<org.apache.spark.sql.Row, SparkRow>) SparkRow::new, SparkGOR.sparkrowEncoder());
            } else {
                if (lng) {
                    ret = dr.map((MapFunction<org.apache.spark.sql.Row, SparkRow>) LongGorSparkRow::new, SparkGOR.sparkrowEncoder());
                } else {
                    ret = dr.map((MapFunction<org.apache.spark.sql.Row, SparkRow>) GorSparkRow::new, SparkGOR.sparkrowEncoder());
                }
            }
        } else {
            ret = (Dataset<? extends Row>) dataset;
        }
        return ret;
    }

    @Override
    public boolean pushdownFilter(String gorwhere) {
        if(pushdownGorPipe!=null) pushdownGor("where "+gorwhere);
        else {
            StructType st = dataset.schema();
            StructField[] fields = st.fields();
            nor = nor | checkNor(fields);
            String[] headersplit = Arrays.stream(fields).map(StructField::name).toArray(String[]::new);
            String[] ctypes = Arrays.stream(st.fields()).map(f -> dmap.get(f.dataType())).toArray(String[]::new);
            dataset = dataset.filter((FilterFunction) (nor ? new NorFilterFunction(gorwhere, headersplit, ctypes) : new GorFilterFunction(gorwhere, headersplit, ctypes)));
        }
        return true;
    }

    @Override
    public boolean pushdownCalc(String formula, String colName) {
        if(pushdownGorPipe!=null) pushdownGor("calc " + colName + " " + formula);
        else {
            StructType st = dataset.schema();
            StructField[] st_fields = st.fields();
            nor = nor | checkNor(st_fields);
            String[] headersplit = Arrays.stream(st_fields).map(StructField::name).toArray(String[]::new);
            String[] ctypes = Arrays.stream(st.fields()).map(f -> dmap.get(f.dataType())).toArray(String[]::new);
            DataType[] dataTypes = Arrays.stream(st.fields()).map(StructField::dataType).toArray(DataType[]::new);
            FilterParams fp = new FilterParams(formula, headersplit, ctypes);

            OptionalInt oi = IntStream.range(0, headersplit.length).filter(i -> headersplit[i].equalsIgnoreCase(colName)).findFirst();
            StructField[] fields = oi.isPresent() ? new StructField[headersplit.length] : new StructField[headersplit.length + 1];
            IntStream.range(0, headersplit.length).forEach(i -> fields[i] = new StructField(headersplit[i], dataTypes[i], true, Metadata.empty()));

            GorMapFunction gmp = nor ? new NorMapFunction(fp, oi) : new GorMapFunction(fp, oi);
            String ctype = gmp.getCalcType();
            DataType type = dsmap.get(ctype);
            fields[oi.isPresent() ? oi.getAsInt() : fields.length - 1] = new StructField(colName, type, true, Metadata.empty());

            StructType schema = new StructType(fields);
            ExpressionEncoder<org.apache.spark.sql.Row> encoder = RowEncoder.apply(schema);

            dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).map(gmp, encoder);
            setHeader(correctHeader(dataset.columns()));
        }
        return true;
    }

    @Override
    public boolean pushdownSelect(String[] cols) {
        return false;
    }

    @Override
    public boolean pushdownWrite(String filename) {
        it = null;
        this.parquetPath = filename;
        return true;
    }

    @Override
    public boolean pushdownGor(String gor) {
        if (pushdownGorPipe == null) {
            if(gor.startsWith("gatk")) {
                String command = gor.substring(5);
                if(command.startsWith("haplotypecaller")) {
                    SparkSession sparkSession = gorSparkSession.getSparkSession();
                    JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
                    //JavaRDD<GATKRead> javaRdd = dataset.toJavaRDD();
                    //HaplotypeCallerSpark.callVariantsWithHaplotypeCallerAndWriteOutput(jsc, javaRdd);
                }
            } else if(gor.startsWith("pipe")) {
                Map<String, String> options = new HashMap<>();
                String cmd = gor.substring(4).trim();
                String[] pipe_options = cmd.split(" ");
                for(String popt : pipe_options) {
                    String[] psplit = popt.split("=");
                    if(psplit[1].startsWith("'")) options.put(psplit[0],psplit[1].substring(1,psplit[1].length()-1));
                    else options.put(psplit[0],psplit[1]);
                }
                dataset = Glow.transform("pipe", (Dataset<org.apache.spark.sql.Row>) dataset, options);
            } else if(gor.startsWith("split_multiallelics")) {
                Map<String, String> options = new HashMap<>();
                dataset = Glow.transform("split_multiallelics", (Dataset<org.apache.spark.sql.Row>) dataset, options);
            } else pushdownGorPipe = gor;
        } else {
            pushdownGorPipe += "|" + gor;
        }
        return true;
    }

    @Override
    public boolean pushdownTop(int limit) {
        if(pushdownGorPipe!=null) pushdownGor("top " + limit);
        else dataset = dataset.limit(limit);
        return true;
    }
}
