package gorsat.process;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.DataFormatException;

import com.databricks.spark.xml.util.XSDToSchema;
import gorsat.commands.PysparkAnalysis;
import io.projectglow.Glow;
import io.projectglow.common.VariantSchemas;
import io.projectglow.transformers.blockvariantsandsamples.VariantSampleBlockMaker;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import org.apache.spark.sql.expressions.Window;
import org.gorpipe.exceptions.GorResourceException;
import org.gorpipe.gor.binsearch.RowBuffer;
import org.gorpipe.gor.driver.DataSource;
import org.gorpipe.gor.driver.providers.stream.datatypes.bam.BamIterator;
import org.gorpipe.gor.model.*;
import org.gorpipe.gor.model.FileReader;
import org.gorpipe.gor.model.Row;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.gor.table.util.PathUtils;
import org.gorpipe.spark.*;
import gorsat.Commands.Analysis;
import gorsat.Commands.CommandParseUtilities;
import gorsat.DynIterator;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.*;
import org.gorpipe.spark.udfs.CharToDoubleArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Created by sigmar on 12/02/16.
 */
public class SparkRowSource extends ProcessSource {
    private static final Logger log = LoggerFactory.getLogger(SparkRowSource.class);

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

    private void initFileRoot(GorSession gpSession) {
        String root = gpSession.getProjectContext().getRoot();
        String cachedir = gpSession.getProjectContext().getCacheDir();
        if (root != null && root.length() > 0) {
            int i = root.indexOf(' ');
            if (i == -1) i = root.length();
            fileroot = root.substring(0, i);
            cachepath = cachedir != null && cachedir.length() > 0 ? cachedir : "result_cache";
            if(!PathUtils.isAbsolutePath(cachepath)) cachepath = PathUtils.resolve(fileroot,cachepath);
        }
    }

    private StructType loadSchema(String ddl, String root) {
        if (ddl.toLowerCase().endsWith(".xsd")) {
            if (!PathUtils.isAbsolutePath(ddl)) ddl = PathUtils.resolve(root,ddl);
            return XSDToSchema.read(ddl);
        } else if(ddl.startsWith("<")) {
            return XSDToSchema.read(ddl);
        }
        return StructType.fromDDL(ddl);
    }

    public SparkRowSource(String sql, String profile, String parquet, String type, boolean nor, GorSparkSession gpSession, final String filter, final String filterFile, final String filterColumn, final String splitFile, final String chr, final int pos, final int end, boolean usestreaming, String jobId, boolean useCpp, String parts, int buckets, boolean tag, String ddl, String format, String option) throws IOException, DataFormatException {
        init();
        this.sql = sql;
        this.jobId = jobId;
        this.tag = tag;

        this.buckets = buckets != -1 ? buckets : null;
        this.parts = parts;

        this.gorSparkSession = gpSession;
        this.nor = nor;

        if (format==null || format.length()==0) {
            for (Map.Entry e : jdbcmap.entrySet()) {
                var key = "[" + e.getKey() + "].";
                var i = CommandParseUtilities.quoteSafeIndexOf(sql, " " + key, false, 0);
                if (i > 0) {
                    sql = sql.replace(key, "");
                    if (option == null || option.length() == 0) option = "url=" + e.getValue();
                    else option += ";url=" + e.getValue();
                    break;
                }
            }
        }

        var options = new HashMap<String,String>();
        if(option!=null) {
            if(option.startsWith("'")) {
                option = option.substring(1,option.length()-1);
            }
            for(String split : option.split(";")) {
                String splittrim = split.trim();
                int ie = splittrim.indexOf('=');
                String key = splittrim.substring(0,ie);
                String val = splittrim.substring(ie+1);
                options.put(key,val);
            }
        }

        if (parquet != null && Files.exists(Paths.get(parquet))) {
            dataset = gpSession.getSparkSession().read().parquet(parquet);
        } else if(format != null) {
            initFileRoot(gpSession);
            var dataFrameReader = gpSession.getSparkSession().read().format(format);
            for (Map.Entry<String,String> entry : options.entrySet()) {
                dataFrameReader = dataFrameReader.option(entry.getKey(),entry.getValue());
            }
            if(ddl!=null) {
                StructType schema = loadSchema(ddl, fileroot);
                dataFrameReader = dataFrameReader.schema(schema);
            }
            if(format.equals("jdbc")) {
                if (sql.toLowerCase().startsWith("select ")) {
                    dataset = dataFrameReader.option("query",sql).load();
                } else {
                    dataset = dataFrameReader.option("dbtable",sql).load();
                }
            } else if(format.contains("redis")) {
                var sqlow = sql.toLowerCase();
                if (sqlow.startsWith("select ")) {
                    int i = sqlow.indexOf(" from ")+6;
                    String table = sql.substring(i,sql.indexOf(' ',i)).trim();
                    var sqlhash = "g"+sql.hashCode();
                    sql = sql.replace(" from "+table," from "+sqlhash);
                    dataFrameReader.option("table",table).load().createOrReplaceTempView(sqlhash);
                    dataset = gorSparkSession.getSparkSession().sql(sql);
                } else {
                    dataset = sql.equals("dummy") || options.containsKey("table") ? dataFrameReader.load() : dataFrameReader.option("table",sql).load();
                }
            } else if(sql.toLowerCase().startsWith("select ")) {
                dataset = dataFrameReader.load();
            } else {
                dataset = dataFrameReader.load(sql);
            }
        } else {
            this.type = type;
            commands = new ArrayList<>();

            this.chr = chr;
            this.start = pos;
            this.end = end;

            initFileRoot(gpSession);

            String[] cmdsplit = CommandParseUtilities.quoteCurlyBracketsSafeSplit(sql, ' ');
            commands.addAll(Arrays.asList(cmdsplit));

            boolean bamvcf = type != null && (type.equals("bam") || type.equals("sam") || type.equals("cram") || type.equals("vcf"));
            List<String> headercommands = bamvcf ? seekCmd(null, 0, -1) : seekCmd(chr, start, end);

            String standalone = System.getProperty("sm.standalone");

            DriverBackedFileReader fileReader = (DriverBackedFileReader) gorSparkSession.getProjectContext().getFileReader();
            inner = p -> {
                if (p.startsWith("(")) {
                    String[] cmdspl = CommandParseUtilities.quoteCurlyBracketsSafeSplit(p.substring(1, p.length() - 1), ' ');
                    return Arrays.stream(cmdspl).map(inner).map(gorfunc).map(parqfunc).collect(Collectors.joining(" ", "(", ")"));
                } else return p;
            };
            gorpred = SparkRowUtilities.getFileEndingPredicate();
            gorfunc = p -> {
                if (gorpred.test(p)) {
                    boolean nestedQuery = p.startsWith("<(");
                    String fileName;
                    List<Instant> inst;
                    if (nestedQuery) {
                        fileName = p.substring(2, p.length() - 1);
                        var scmdsplit = CommandParseUtilities.quoteCurlyBracketsSafeSplit(fileName, ' ');
                        inst = Arrays.stream(scmdsplit).flatMap(gorfileflat).filter(gorpred).map(sp -> PathUtils.isAbsolutePath(sp) ? sp : PathUtils.resolve(fileroot,sp)).map(sp -> {
                            try {
                                return Instant.ofEpochMilli(fileReader.resolveUrl(sp).getSourceMetadata().getLastModified());
                            } catch (IOException e) {
                                // Failed getLastModifiedTime are not part of the signature
                                return null;
                            }
                        }).filter(Objects::nonNull).collect(Collectors.toList());
                    } else {
                        RowDataType rdt;
                        try {
                            rdt = SparkRowUtilities.translatePath(p, fileroot, standalone, fileReader);
                        } catch (IOException e) {
                            throw new GorResourceException("Unable to read from link file", p, e);
                        }
                        fileName = rdt.path;
                        inst = rdt.getTimestamp();
                    }
                    return SparkRowUtilities.generateTempViewName(fileName, usestreaming, filter, chr, pos, end, inst);
                }
                return p;
            };
            gorfileflat = p -> p.startsWith("(") ? Arrays.stream(CommandParseUtilities.quoteCurlyBracketsSafeSplit(p.substring(1, p.length() - 1), ' ')).flatMap(gorfileflat).filter(gorpred) : Stream.of(p);
            parqfunc = p -> {
                try {
                    if (p.toLowerCase().endsWith(".link")) {
                        p = SparkRowUtilities.translatePath(p, fileroot, standalone, fileReader).path;
                    }
                    if (p.toLowerCase().endsWith(".parquet") && !(p.toLowerCase().startsWith("parquet.") || p.startsWith("s3a://") || p.startsWith("s3://"))) {
                        String fileName = SparkRowUtilities.translatePath(p, fileroot, standalone, fileReader).path;
                        return "parquet.`" + fileName + "`";
                    } else return p;
                } catch (IOException e) {
                    throw new GorResourceException("Unable to read from link file", p, e);
                }
            };

            boolean isSql = headercommands.get(0).equalsIgnoreCase("select");
            String[] fileNames;
            String cacheFile = null;
            if (isSql) {
                cmdsplit = headercommands.stream().filter(p -> p.length() > 0).map(parqfunc).toArray(String[]::new);
                commands.clear();
                commands.addAll(Arrays.asList(cmdsplit));
                sql = Arrays.stream(cmdsplit).map(inner).map(gorfunc).collect(Collectors.joining(" "));
                fileNames = Arrays.stream(cmdsplit).flatMap(gorfileflat).filter(gorpred).toArray(String[]::new);
                for (String fn : fileNames) {
                    if (gorSparkSession.getSystemContext().getServer()) DriverBackedGorServerFileReader.validateServerFileName(fn, fileroot.toString(), true);
                    StructType schema = ddl!=null ? loadSchema(ddl, fileroot) : null;
                    SparkRowUtilities.registerFile(new String[]{fn}, profile,null, gpSession, standalone, fileroot, cachepath, usestreaming, filter, filterFile, filterColumn, splitFile, nor, chr, pos, end, jobId, cacheFile, useCpp, tag, schema, options);
                }
                dataset = gorSparkSession.getSparkSession().sql(sql);
            } else {
                fileNames = headercommands.toArray(new String[0]);
                if (fileNames.length == 1 && fileNames[0].toLowerCase().endsWith(".parquet")) {
                    String parq = SparkRowUtilities.translatePath(fileNames[0], fileroot, standalone, fileReader).path;
                    dataset = gpSession.getSparkSession().read().parquet(parq);
                } else {
                    StructType schema = ddl != null ? loadSchema(ddl, fileroot) : null;
                    dataset = SparkRowUtilities.registerFile(fileNames, null, profile, gpSession, standalone, fileroot, cachepath, usestreaming, filter, filterFile, filterColumn, splitFile, nor, chr, pos, end, jobId, cacheFile, useCpp, tag, schema, options);
                }
            }

            if (chr != null) {
                if (end != -1) {
                    dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) <= end && row.getInt(1) >= pos);
                } else {
                    dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) >= pos);
                }
            }
            gorSparkSession.getSparkSession().sparkContext().setJobDescription( sql);
        }
        boolean checknor = checkNor(dataset.schema().fields());
        setHeader((nor || checknor ? "chrNOR\tposNOR\t" : "") + correctHeader(dataset.columns()));
    }

    String sql;
    String errorStr = "";
    List<String> commands;
    String type;
    boolean isGorRow = false;
    Dataset<? extends org.apache.spark.sql.Row> dataset;
    Iterator<Row> it;
    boolean nor;
    ProcessBuilder pb;
    Process p;
    String fileroot = null;
    String cachepath = null;
    String parquetPath = null;
    String dictPath = null;
    int pcacomponents = 10;
    String pushdownGorPipe = null;
    GorSparkSession gorSparkSession;

    Map<DataType, String> dmap = new HashMap<>();
    Map<String, DataType> dsmap = new HashMap<>();

    String chr;
    int start;
    int end;

    String jobId = "-1";

    Integer buckets;
    String parts;
    boolean tag;

    List<PysparkAnalysis> pysparkAnalyses = new ArrayList<>();

    java.util.function.Function<String, String> inner;
    java.util.function.Function<String, String> gorfunc;
    java.util.function.Predicate<String> gorpred;
    java.util.function.Function<String, String> parqfunc;
    java.util.function.Function<String, Stream<String>> gorfileflat;

    public Dataset<? extends org.apache.spark.sql.Row> getDataset() {
        return dataset;
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

    public static Dataset<Row> gorpipe(Dataset<? extends org.apache.spark.sql.Row> dataset, String gor) {
        String inputHeader = String.join("\t", dataset.schema().fieldNames());
        boolean nor = checkNor(dataset.schema().fields());
        Dataset<? extends Row> dr = (Dataset<? extends Row>) dataset;//checkRowFormat(dataset);

        GorSpark gs = new GorSparkMaterialize(inputHeader, nor, SparkGOR.sparkrowEncoder().schema(), gor, null, null, "-1", 100);
        GorSparkRowInferFunction gi = new GorSparkRowInferFunction();
        Row row = ((Dataset<Row>) dr).mapPartitions(gs, SparkGOR.gorrowEncoder()).limit(100).reduce(gi);
        if (row.chr != null) row = gi.infer(row, row);
        StructType schema = schemaFromRow(gs.query().getHeader().split("\t"), row);

        ExpressionEncoder encoder = RowEncoder.apply(schema);
        gs = new GorSpark(inputHeader, nor, schema, gor, null, null, "-1");
        return ((Dataset<Row>) dr).mapPartitions(gs, encoder);
    }

    public void gor() {
        String inputHeader = super.getHeader();

        boolean nor = checkNor(dataset.schema().fields());
        Dataset<? extends Row> dr = checkRowFormat(dataset);

        String uri = gorSparkSession.getRedisUri();
        GorSpark gs = new GorSparkMaterialize(inputHeader, nor, SparkGOR.sparkrowEncoder().schema(), pushdownGorPipe, gorSparkSession.getProjectContext().getProjectRoot(), uri, jobId, 100);
        GorSparkRowInferFunction gi = new GorSparkRowInferFunction();
        Row row = ((Dataset<Row>) dr).mapPartitions(gs, SparkGOR.gorrowEncoder()).limit(100).reduce(gi);
        if (row.chr != null) row = gi.infer(row, row);
        StructType schema = schemaFromRow(gs.query().getHeader().split("\t"), row);

        this.setHeader(correctHeader(schema.fieldNames()));
        ExpressionEncoder encoder = RowEncoder.apply(schema);
        gs = new GorSpark(inputHeader, nor, schema, pushdownGorPipe, gorSparkSession.getProjectContext().getProjectRoot(), uri, jobId);
        pushdownGorPipe = null;
        dataset = ((Dataset<Row>) dr).mapPartitions(gs, encoder);

        nor = checkNor(dataset.schema().fields());
        setHeader((nor ? "chrNOR\tposNOR\t" : "") + correctHeader(dataset.columns()));

        //GorSparkSession.getSparkSession().sparkContext().setJobGroup("group gor", pushdownGorPipe, true);
    }

    static Map<String,String> jdbcmap = new HashMap<>();
    static Map<String, DataType> tmap = new HashMap<>();
    static {
        tmap.put("S", StringType);
        tmap.put("I", IntegerType);
        tmap.put("D", DoubleType);

        var gordbcreds = System.getProperty("gor.db.credentials","/opt/nextcode/gor-scripts/config/gor.db.credentials");
        var path = Paths.get(gordbcreds);
        if (Files.exists(path)) {
            try {
                Files.lines(path).skip(1).forEach(l -> {
                    var spl = l.split("\t");
                    if (spl.length<4) spl = l.split("\\t");
                    jdbcmap.put(spl[0],spl[2]+"?user="+spl[3]+"&password="+spl[4]);
                });
            } catch (IOException e) {
                log.error("No jdbc urls loaded", e);
            }
        }
    }

    public static StructType schemaFromRow(String[] header, Row row) {
        return new StructType(IntStream.range(0, row.numCols()).mapToObj(i -> new StructField(header[i], tmap.get(row.stringValue(i)), true, Metadata.empty())).toArray(StructField[]::new));
    }

    public String checkNested(String cmd, GorSession gpSession, String[] errorStr) {
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
                fileroot = root.substring(0, i);
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
            if (fileroot != null) pb.directory(Path.of(fileroot).toFile());
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
                ChromoLookup lookup = ProcessRowSource.createChromoLookup();
                try {
                    it = new VcfGzGenomicIterator(lookup, "filename", br) {
                        @Override
                        public boolean seek(String seekChr, int seekPos) {
                            return seek(seekChr, seekPos, lookup.chrToLen(seekChr));
                        }

                        @Override
                        public boolean seek(String seekChr, int seekPos, int endPos) {
                            try {
                                reader.close();
                                if (seekChr != null && this.chrNameSystem != VcfGzGenomicIterator.ChrNameSystem.WITH_CHR_PREFIX)
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
                ChromoLookup lookup = ProcessRowSource.createChromoLookup();
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
                bamit.init(lookup, samreader, false);
                bamit.it = bamit.reader.iterator();
            }
        } catch (IOException e) {
            throw new RuntimeException("unable to get header from process " + commands.get(0), e);
        }
    }

    public static boolean checkNor(StructField[] fields) {
        return fields.length == 1 || !(fields[0].name().equalsIgnoreCase("chrom") && fields[1].dataType() == IntegerType);
    }

    void writeDictionary(org.apache.hadoop.fs.Path resolvedPath, org.apache.hadoop.fs.Path dictPath) throws IOException {
        Dataset<org.apache.spark.sql.Row> ds = gorSparkSession.getSparkSession().read().format("csv").load(resolvedPath +"/*.meta").withColumn("inputFile", org.apache.spark.sql.functions.input_file_name());
        Dataset<String> dss = ds.selectExpr("inputFile","_c0 as meta").where("meta like '## RANGE%'").map((MapFunction<org.apache.spark.sql.Row, String>) r -> {
            String first = r.getString(0);
            int l = first.lastIndexOf('/');
            String name = first.substring(l+1);
            if(first.startsWith("file:")) {
                first = name;
                l = -1;
            }
            return first.substring(0,first.length()-5)+"\t"+first.substring(l+6,l+11)+"\t"+r.getString(1).substring(10);
        }, Encoders.STRING()).coalesce(1);
        List<String> lss = dss.collectAsList();

        Configuration conf = new Configuration();
        FileSystem fs = dictPath.getFileSystem(conf);
        if(fs.getFileStatus(dictPath).isDirectory()) {
            dictPath = new org.apache.hadoop.fs.Path(dictPath,"dict.gord");
        }
        FSDataOutputStream is = fs.create(dictPath);
        for(String l : lss) {
            is.writeBytes(l);
            is.write('\n');
        }
        is.close();
    }

    private RowBuffer rowBuffer = null;

    private Dataset<org.apache.spark.sql.Row> gttranspose(Dataset<org.apache.spark.sql.Row> ds) {
        int varcount = ds.mapPartitions((MapPartitionsFunction<org.apache.spark.sql.Row, Integer>) ir -> {
            Iterable<org.apache.spark.sql.Row> newIterable = () -> ir;
            int count = (int)StreamSupport.stream(newIterable.spliterator(), false).count();
            return Collections.singletonList(count).iterator();
        }, Encoders.INT()).first();
        StructType schema = new StructType();
                if(ds.schema().length()>1) schema = schema.add("pn", DataTypes.StringType);
                schema = schema.add("values",new VectorUDT());
        int schemaLen = ds.schema().length();
        ExpressionEncoder<org.apache.spark.sql.Row> vectorEncoder = RowEncoder.apply(schema);
        Dataset<org.apache.spark.sql.Row> dv = ds.mapPartitions((MapPartitionsFunction<org.apache.spark.sql.Row, org.apache.spark.sql.Row>) ir -> {
            double[][] mat = null;
            Iterator<org.apache.spark.sql.Row> it = Collections.emptyIterator();
            String[] pns = null;
            int start = 0;
            while(ir.hasNext()) {
                org.apache.spark.sql.Row row = ir.next();
                String strvec;
                if(schemaLen>1) {
                    pns = row.getString(0).split(",");
                    strvec = row.getString(1).substring(1);
                } else strvec = row.getString(0).substring(1);
                int len = strvec.length();
                if(mat==null) {
                    mat = new double[len][];
                    for(int i = 0; i < len; i++) {
                        mat[i] = new double[varcount];
                    }
                }
                //if(start*len > mat.length) throw new RuntimeException("len " + len + " " + mat.length + "  " + varcount);
                for(int i = 0; i < len; i++) {
                    mat[i][start] = strvec.charAt(i)-'0';
                }
                start++;
            }
            if(mat!=null) {
                List<org.apache.spark.sql.Row> lv = new ArrayList<>(mat.length);

                for(int i = 0; i < mat.length; i++) {
                    //org.apache.spark.sql.Row.fromTuple()
                    org.apache.spark.ml.linalg.Vector vector = Vectors.dense(mat[i]);
                    org.apache.spark.sql.Row row;
                    if(pns!=null) row = RowFactory.create(pns[i],vector);
                    else row = RowFactory.create(vector);
                    lv.add(row);
                }
                return lv.stream().iterator();
            }
            return it;
        }, vectorEncoder);
        return dv;
    }

    @Override
    public boolean hasNext() {
        if (it == null) {
            if (parquetPath != null) {
                try {
                    boolean exists;
                    FileReader fileReader = gorSparkSession.getProjectContext().getFileReader();
                    String resolvedPath;
                    if(fileReader instanceof DriverBackedFileReader) {
                        DriverBackedFileReader driverBackedFileReader = (DriverBackedFileReader)fileReader;
                        DataSource ds = driverBackedFileReader.resolveUrl(parquetPath);
                        exists = ds.exists();
                        URI uri = URI.create(ds.getSourceReference().getUrl());
                        if(!uri.isAbsolute() && fileroot!=null) {
                            resolvedPath = PathUtils.resolve(fileroot,parquetPath);
                        } else {
                            resolvedPath = ds.getSourceReference().getUrl();
                        }
                    } else {
                        if (fileroot != null && !PathUtils.isAbsolutePath(parquetPath)) {
                            parquetPath = PathUtils.resolve(fileroot,parquetPath);
                        }
                        exists = fileReader.exists(parquetPath);
                        resolvedPath = parquetPath;
                    }
                        /*if (!checkNor(dataset.schema().fields())) {
                            String path = pPath.resolve(pPath.getFileName().toString() + ".gorp").toAbsolutePath().normalize().toString();
                            Encoder<org.apache.spark.sql.Row> enc = (Encoder<org.apache.spark.sql.Row>) dataset.encoder();
                            GorpWriter gorpWriter = new GorpWriter(path);
                            dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).mapPartitions(gorpWriter, enc);
                        }*/

                    if(!exists) {
                        if (parquetPath.endsWith(".pca")) {
                            PCA pca = new PCA();
                            pca.setK(pcacomponents);
                            pca.setOutputCol("pca");
                            pca.setInputCol("values");
                            PCAModel pcamodel = pca.fit(dataset);
                            try {
                                pcamodel.save(parquetPath);
                            } catch (IOException e) {
                                throw new GorResourceException("Unable to save pcamodel file", parquetPath, e);
                            }
                        } else {
                            Arrays.stream(dataset.columns()).filter(c -> c.contains("(")).forEach(c -> dataset = dataset.withColumnRenamed(c, c.replace('(', '_').replace(')', '_')));
                            DataFrameWriter dfw = dataset.write();
                            if (parts != null) {
                                if (buckets != null) {
                                    dfw = dfw.bucketBy(buckets, parts);
                                } else {
                                    dfw = dfw.partitionBy(parts.split(","));
                                }
                            }
                            boolean gorformat = parquetPath.toLowerCase().endsWith(".gorz");
                            dfw = gorformat ? dfw.format("gor") : dfw.format("parquet");
                            dfw.mode(SaveMode.Overwrite).save(resolvedPath);

                            if (gorformat) {
                                org.apache.hadoop.fs.Path hp = new org.apache.hadoop.fs.Path(resolvedPath);
                                if (parquetPath.equals(dictPath)) {
                                    writeDictionary(hp, hp);
                                } else {
                                    if (fileroot != null && !PathUtils.isAbsolutePath(dictPath)) {
                                        dictPath = PathUtils.resolve(fileroot,dictPath);
                                    }
                                    org.apache.hadoop.fs.Path dp = new org.apache.hadoop.fs.Path(dictPath);
                                    writeDictionary(hp, dp);
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new GorResourceException("Unable to get datasource", parquetPath, e);
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
    public boolean seek(String seekChr, int seekPos) {
        return true;
    }

    @Override
    public void close() {
        pysparkAnalyses.forEach(PysparkAnalysis::close);
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
            if (fileroot != null) pb.directory(Path.of(fileroot).toFile());
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
        if (pushdownGorPipe != null) pushdownGor("where " + gorwhere);
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
        if (formula.startsWith("udf")) {
            String newformula = formula.substring(4, formula.length() - 1).trim();
            int i = newformula.indexOf('(');
            String udfname = newformula.substring(0,i);
            String[] args = newformula.substring(i+1,newformula.length()-1).split(",");
            var colseq = ScalaUtils.columns(args);
            dataset = dataset.withColumn(colName,functions.callUDF(udfname,colseq));
        } else if (formula.toLowerCase().startsWith("normalize")) {
            String oldcolname = formula.substring(10, formula.length() - 1).trim();
            Dataset<org.apache.spark.sql.Row> ds = (Dataset<org.apache.spark.sql.Row>) dataset;
            Normalizer normalizer = new Normalizer();
            normalizer.setInputCol(oldcolname);
            normalizer.setOutputCol(colName);
            dataset = normalizer.transform(ds);
        } else if (formula.toLowerCase().startsWith("pcatransform")) {
            //int c = formula.indexOf(',');
            //String oldcolname = formula.substring(13,formula.length()-1).trim();
            String modelpath = formula.substring(13,formula.length()-1).trim();
            if(modelpath.startsWith("'")) modelpath = modelpath.substring(1,modelpath.length()-1);
            Dataset<org.apache.spark.sql.Row> ds = (Dataset<org.apache.spark.sql.Row>)dataset;
            dataset = pcatransform(ds, modelpath).withColumnRenamed("pca",colName);
        } else if (formula.toLowerCase().startsWith("chartodoublearray")) {
            if (pushdownGorPipe != null) gor();
            CharToDoubleArray cda = new CharToDoubleArray();
            UserDefinedFunction udf1 = org.apache.spark.sql.functions.udf(cda, DataTypes.createArrayType(DataTypes.DoubleType));
            String colRef = formula.substring("chartodoublearray".length() + 1, formula.length() - 1);
            dataset = dataset.withColumn(colName, udf1.apply(dataset.col(colRef)));
        } else if (pushdownGorPipe != null) {
            pushdownGor("calc " + colName + " " + formula);
        } else {
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
        if(parquetPath==null) {
            int id = filename.indexOf("-pca ");
            if (id != -1) {
                int k = id + 5;
                char c = filename.charAt(k);
                while (c == ' ') c = filename.charAt(++k);
                while (k < filename.length() && c != ' ') c = filename.charAt(k++);
                String pcompstr = filename.substring(id + 5, k).trim();
                pcacomponents = Integer.parseInt(pcompstr);
                this.parquetPath = filename.substring(k).trim();
                if (this.parquetPath.length() == 0) {
                    this.parquetPath = gorSparkSession.getProjectContext().getFileCache().tempLocation(jobId, CommandParseUtilities.getExtensionForQuery(sql.startsWith("<(") ? "spark " + sql : sql, false));
                }
            } else {
                id = filename.indexOf("-d ");
                if(id>=0) {
                    int li = filename.indexOf(' ', id+3);
                    if(li==-1) li = filename.length();
                    this.parquetPath = filename.substring(id+3,li).trim();
                    this.dictPath = li==filename.length() ? parquetPath : filename.substring(li+1).trim();
                } else {
                    this.parquetPath = filename;
                    this.dictPath = filename;
                }
            }
        }
        it = null;
        return true;
    }

    @Override
    public boolean pushdownCmd(String cmd) {
        int i = cmd.indexOf('{');
        String query = cmd.substring(i+1,cmd.length()-1);

        Dataset<? extends Row> dr = checkRowFormat(dataset);

        String inputHeader = String.join("\t", dataset.schema().fieldNames());
        GorSparkExternalFunction gsef = new GorSparkExternalFunction(inputHeader,query,gorSparkSession.getProjectContext().getProjectRoot());
        gsef.setFetchHeader(true);
        Row r = ((Dataset<Row>) dr).mapPartitions(gsef, SparkGOR.gorrowEncoder()).head();
        gsef.setFetchHeader(false);

        GorSparkRowInferFunction gi = new GorSparkRowInferFunction();
        Row row = ((Dataset<Row>) dr).mapPartitions(gsef, SparkGOR.gorrowEncoder()).limit(100).reduce(gi);
        if (row.chr != null) row = gi.infer(row, row);
        StructType schema = schemaFromRow(r.toString().split("\t"), row);

        gsef.setSchema(schema);
        this.setHeader(correctHeader(schema.fieldNames()));
        ExpressionEncoder encoder = RowEncoder.apply(schema);
        dataset = ((Dataset<Row>) dr).mapPartitions(gsef, encoder);
        return true;
    }

    public static Dataset<org.apache.spark.sql.Row> analyse(Dataset<org.apache.spark.sql.Row> dataset, String gor) {
        Dataset<org.apache.spark.sql.Row> ret = null;
        if (gor.startsWith("gatk")) {
            String command = gor.substring(5);
            if (command.startsWith("haplotypecaller")) {
                //SparkSession sparkSession = gorSparkSession.getSparkSession();
                //JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

                //JavaRDD<GATKRead> javaRdd = dataset.toJavaRDD();
                //HaplotypeCallerSpark.callVariantsWithHaplotypeCallerAndWriteOutput(jsc, javaRdd);
            }
        } else if (gor.startsWith("pipe")) {
            Map<String, String> options = new HashMap<>();
            String cmd = gor.substring(4).trim();
            String[] pipe_options = cmd.split(" ");
            for (String popt : pipe_options) {
                String[] psplit = popt.split("=");
                if (psplit[1].startsWith("'"))
                    options.put(psplit[0], psplit[1].substring(1, psplit[1].length() - 1));
                else options.put(psplit[0], psplit[1]);
            }
            //ret = Glow.transform("pipe", dataset, options);
        } else if (gor.startsWith("split_multiallelics")) {
            Map<String, String> options = new HashMap<>();
            //ret = Glow.transform("split_multiallelics", dataset, options);
        } else if (gor.startsWith("glowtransform")) {
            Map<String, String> options = new HashMap<>();
            String cmd = gor.substring("glowtransform".length()).trim();
            String[] pipe_options = cmd.split(" ");
            var transformcmd = pipe_options[0].trim();
            for (int i = 0; i < pipe_options.length; i++) {
                var popt = pipe_options[i].trim();
                String[] psplit = popt.split("=");
                if (psplit[1].startsWith("'"))
                    options.put(psplit[0], psplit[1].substring(1, psplit[1].length() - 1));
                else options.put(psplit[0], psplit[1]);
            }
            ret = Glow.transform(transformcmd, dataset, options);
        } else if (gor.startsWith("block_variants_and_samples")) {
            var split = gor.substring("make_sample_blocks".length()).split(" ");
            var variantsPerBlock = Integer.parseInt(split[0].trim());
            var sampleBlockCount = Integer.parseInt(split[1].trim());
            ret = makeVariantAndSampleBlocks(dataset, variantsPerBlock, sampleBlockCount);
        } else if (gor.startsWith("make_sample_blocks")) {
            int sampleCount = Integer.parseInt(gor.substring("make_sample_blocks".length()).trim());
            ret = VariantSampleBlockMaker.makeSampleBlocks(dataset, sampleCount);
        }
        return ret;
    }

    public static Dataset<org.apache.spark.sql.Row> makeVariantAndSampleBlocks(
            Dataset<org.apache.spark.sql.Row> variantDf,
            int variantsPerBlock,
            int sampleBlockCount) {
        var windowSpec = Window
                .partitionBy(VariantSchemas.contigNameField().name(), VariantSchemas.sampleBlockIdField().name())
                .orderBy(VariantSchemas.startField().name(), VariantSchemas.refAlleleField().name(), VariantSchemas.alternateAllelesField().name());

        var seq = ScalaUtils.toSeq(new String[] {"mean", "stdDev"});
        var baseDf = VariantSampleBlockMaker.filterOneDistinctValue(VariantSampleBlockMaker.validateNumValues(variantDf))
                .withColumn(
                        VariantSchemas.sortKeyField().name(),
                        col(VariantSchemas.startField().name()).cast(IntegerType)
                )
                .withColumn(
                        VariantSchemas.headerField().name(),
                        concat_ws(
                                ":",
                                col(VariantSchemas.contigNameField().name()),
                                col(VariantSchemas.startField().name()),
                                col(VariantSchemas.refAlleleField().name()),
                                col(VariantSchemas.alternateAllelesField().name())
                        )
                )
                .withColumn(
                        "stats",
                        io.projectglow.functions.subset_struct(
                                io.projectglow.functions.array_summary_stats(
                                        col(VariantSchemas.valuesField().name())
                                ),
                                seq
                        )
                )
                .withColumn(
                        VariantSchemas.meanField().name(),
                        col("stats.mean")
                )
                .withColumn(
                        VariantSchemas.stdDevField().name(),
                        col("stats.stdDev")
                );

        return VariantSampleBlockMaker.makeSampleBlocks(baseDf, sampleBlockCount)
                .withColumn(
                        VariantSchemas.sizeField().name(),
                        size(col(VariantSchemas.valuesField().name()))
                )
                .withColumn(
                        VariantSchemas.headerBlockIdField().name(),
                        concat_ws(
                                "_",
                                lit("chr"),
                                col(VariantSchemas.contigNameField().name()),
                                lit("block"),
                                ((row_number().over(windowSpec).minus(1)).divide(variantsPerBlock)).cast(IntegerType)
                        )
                )
                .select(
                        col(VariantSchemas.headerField().name()),
                        col(VariantSchemas.sizeField().name()),
                        col(VariantSchemas.valuesField().name()),
                        col(VariantSchemas.headerBlockIdField().name()),
                        col(VariantSchemas.sampleBlockIdField().name()),
                        col(VariantSchemas.sortKeyField().name()),
                        col(VariantSchemas.meanField().name()),
                        col(VariantSchemas.stdDevField().name())
                );
    }

    private Dataset<org.apache.spark.sql.Row> pcatransform(Dataset<org.apache.spark.sql.Row> dataset, String modelpath) {
        PCAModel pcamodel = PCAModel.load(modelpath);
        Dataset<org.apache.spark.sql.Row> pcaresult = pcamodel.transform(dataset).select("pn","pca");
        return pcaresult;
    }

    @Override
    public boolean pushdownGor(String gor) {
        if (gor.startsWith("rename")) {
            if (pushdownGorPipe != null) gor();
            String[] split = gor.substring("rename".length()).trim().split(" ");
            dataset = dataset.withColumnRenamed(split[0], split[1]);
        } else if (gor.startsWith("repatition")) {
            if (pushdownGorPipe != null) gor();
            String[] split = gor.substring("repatition".length()).trim().split(" ");
            try {
                int val = Integer.parseInt(split[1]);
                dataset = dataset.repartition(val);
            } catch(Exception e) {
                dataset.repartition();
            }
        } else if (gor.toLowerCase().startsWith("selectexpr ")) {
            String[] selects = gor.substring("selectexpr".length()).trim().split(",");
            dataset = dataset.selectExpr(selects);
        } else if (gor.toLowerCase().startsWith("gttranspose")) {
            dataset = gttranspose((Dataset<org.apache.spark.sql.Row>)dataset);
        } else if (gor.toLowerCase().startsWith("pcatransform ")) {
            String pcamodel = gor.substring("pcatransform".length()).trim();
            dataset = pcatransform((Dataset<org.apache.spark.sql.Row>)dataset, pcamodel);
        } else if (gor.toLowerCase().startsWith("normalize ")) {
            String colname = gor.substring("normalize".length()).trim();
            Dataset<org.apache.spark.sql.Row> ds = (Dataset<org.apache.spark.sql.Row>)dataset;
            Normalizer normalizer = new Normalizer();
            normalizer.setInputCol(colname);
            normalizer.setOutputCol("normalized_"+colname);
            dataset = normalizer.transform(ds);
        } else if (gor.startsWith("pyspark")) {
            if (pushdownGorPipe != null) gor();
            String cmd = gor.substring("pyspark".length());
            var pyspark = new PysparkAnalysis();
            try {
                pysparkAnalyses.add(pyspark);
                dataset = pyspark.pyspark(UUID.randomUUID().toString(), dataset, cmd);
            } catch (IOException | InterruptedException e) {
                throw new GorResourceException("Unable to run spark python command",pyspark.cmdString(),e);
            }
        } else {
            if (pushdownGorPipe == null) {
                Dataset<org.apache.spark.sql.Row> ret = analyse((Dataset<org.apache.spark.sql.Row>) dataset, gor);
                if (ret != null) dataset = ret;
                else pushdownGorPipe = gor;
            } else {
                pushdownGorPipe += "|" + gor;
            }
        }
        return true;
    }

    @Override
    public boolean pushdownTop(int limit) {
        if (pushdownGorPipe != null) pushdownGor("top " + limit);
        else dataset = dataset.limit(limit);
        return true;
    }
}
