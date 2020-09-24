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

import org.apache.spark.sql.*;
import gorsat.commands.PysparkAnalysis;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import org.gorpipe.gor.driver.providers.stream.datatypes.bam.BamIterator;
import org.gorpipe.gor.model.*;
import org.gorpipe.gor.model.Row;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.gor.session.ProjectContext;
import org.gorpipe.spark.*;
import gorsat.Commands.Analysis;
import gorsat.Commands.CommandParseUtilities;
import gorsat.DynIterator;
import gorsat.RowBuffer;
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
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Created by sigmar on 12/02/16.
 */
public class SparkRowSource extends ProcessSource {
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

    public SparkRowSource(String sql, String profile, String parquet, String type, boolean nor, GorSparkSession gpSession, final String filter, final String filterFile, final String filterColumn, final String splitFile, final String chr, final int pos, final int end, boolean usestreaming, String jobId, boolean useCpp, String parts, int buckets, boolean tag) throws IOException, DataFormatException {
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
            String cachedir = gpSession.getProjectContext().getCacheDir();
            if (root != null && root.length() > 0) {
                int i = root.indexOf(' ');
                if (i == -1) i = root.length();
                fileroot = Paths.get(root.substring(0, i));
                cachepath = Paths.get(cachedir != null && cachedir.length() > 0 ? cachedir : "result_cache");
                if(!cachepath.isAbsolute()) cachepath = fileroot.resolve(cachepath);
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
                    if (nestedQuery) {
                        fileName = p.substring(2, p.length() - 1);
                    } else {
                        fileName = SparkRowUtilities.translatePath(p, fileroot, standalone);
                    }
                    return SparkRowUtilities.generateTempViewName(fileName, usestreaming, filter, chr, pos, end);
                }
                return p;
            };
            gorfileflat = p -> p.startsWith("(") ? Arrays.stream(CommandParseUtilities.quoteCurlyBracketsSafeSplit(p.substring(1, p.length() - 1), ' ')).flatMap(gorfileflat).filter(gorpred) : Stream.of(p);
            parqfunc = p -> {
                if (p.toLowerCase().endsWith(".parquet") && !p.toLowerCase().startsWith("parquet.")) {
                    String fileName = SparkRowUtilities.translatePath(p, fileroot, standalone);
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
                    if (gorSparkSession.getSystemContext().getServer()) ProjectContext.validateServerFileName(fn, true);
                    SparkRowUtilities.registerFile(new String[]{fn}, profile,null, gpSession, standalone, fileroot, cachepath, usestreaming, filter, filterFile, filterColumn, splitFile, nor, chr, pos, end, jobId, cacheFile, useCpp, tag);
                }
                dataset = gorSparkSession.getSparkSession().sql(sql);
            } else {
                fileNames = headercommands.toArray(new String[0]);
                dataset = SparkRowUtilities.registerFile(fileNames, null, profile, gpSession, standalone, fileroot, cachepath, usestreaming, filter, filterFile, filterColumn, splitFile, nor, chr, pos, end, jobId, cacheFile, useCpp, tag);
            }

            if (chr != null) {
                if (end != -1) {
                    dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) <= end && row.getInt(1) >= pos);
                } else {
                    dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).filter((FilterFunction<org.apache.spark.sql.Row>) row -> chr.equals(row.getString(0)) && row.getInt(1) >= pos);
                }
            }
            gorSparkSession.getSparkSession().sparkContext().setJobGroup("a|b|gorsql|c", sql, true);
        }
        setHeader((nor ? "chrNOR\tposNOR\t" : "") + correctHeader(dataset.columns()));
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
    Path cachepath = null;
    String parquetPath = null;
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

    static Map<String, DataType> tmap = new HashMap<>();
    static {
        tmap.put("S", StringType);
        tmap.put("I", IntegerType);
        tmap.put("D", DoubleType);
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

                    /*if (!checkNor(dataset.schema().fields())) {
                        String path = pPath.resolve(pPath.getFileName().toString() + ".gorp").toAbsolutePath().normalize().toString();
                        Encoder<org.apache.spark.sql.Row> enc = (Encoder<org.apache.spark.sql.Row>) dataset.encoder();
                        GorpWriter gorpWriter = new GorpWriter(path);
                        dataset = ((Dataset<org.apache.spark.sql.Row>) dataset).mapPartitions(gorpWriter, enc);
                    }*/

                    DataFrameWriter dfw = dataset.write();
                    if (parts != null) {
                        if (buckets != null) {
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
            List<Column> colist = Arrays.stream(args).map(functions::col).collect(Collectors.toList());
            Seq<Column> colseq = JavaConverters.asScalaIterator(colist.iterator()).toSeq();
            dataset = dataset.withColumn(colName,functions.callUDF(udfname,colseq));
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
        it = null;
        this.parquetPath = filename;
        return true;
    }

    @Override
    public boolean pushdownCmd(String cmd) {
        int i = cmd.indexOf('{');
        String query = cmd.substring(i+1,cmd.length()-1);

        Dataset<? extends Row> dr = checkRowFormat(dataset);

        String inputHeader = String.join("\t", dataset.schema().fieldNames());
        GorSparkExternalFunction gsef = new GorSparkExternalFunction(inputHeader,query,null/*gorSparkSession.getProjectContext().getRoot()*/);
        gsef.setFetchHeader(true);
        Row r = ((Dataset<Row>) dr).mapPartitions(gsef, SparkGOR.gorrowEncoder()).head();
        gsef.setFetchHeader(false);

        GorSparkRowInferFunction gi = new GorSparkRowInferFunction();
        Row row = ((Dataset<Row>) dr).mapPartitions(gsef, SparkGOR.gorrowEncoder()).limit(100).reduce(gi);
        if (row.chr != null) row = gi.infer(row, row);
        StructType schema = schemaFromRow(r.toString().split("\t"), row);

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
        } else if (gor.startsWith("block_variants_and_samples")) {
            Map<String, String> options = new HashMap<>();
            String cmd = gor.substring("block_variants_and_samples".length()).trim();
            String[] pipe_options = cmd.split(" ");
            for (String popt : pipe_options) {
                String[] psplit = popt.split("=");
                if (psplit[1].startsWith("'"))
                    options.put(psplit[0], psplit[1].substring(1, psplit[1].length() - 1));
                else options.put(psplit[0], psplit[1]);
            }
            //ret = Glow.transform("block_variants_and_samples", dataset, options);
        } else if (gor.startsWith("make_sample_blocks")) {
            int sampleCount = Integer.parseInt(gor.substring("make_sample_blocks".length()).trim());
            //ret = VariantSampleBlockMaker.makeSampleBlocks(dataset, sampleCount);
        }
        return ret;
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
        } else if (gor.startsWith("pyspark")) {
            if (pushdownGorPipe != null) gor();
            String cmd = gor.substring("pyspark".length());
            try {
                dataset = PysparkAnalysis.pyspark(dataset, cmd);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
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
