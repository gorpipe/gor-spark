package org.gorpipe.spark;

import gorsat.process.PipeOptions;
import gorsat.process.SparkPipeInstance;
import io.projectglow.Glow;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.session.GorSession;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class UTestGorSparkQuery {
    SparkSession spark;
    GorSparkSession sparkGorSession;
    SparkPipeInstance pi;

    @Before
    public void init() {
        spark = SparkSession.builder()
                .config("spark.hadoop.fs.s3a.endpoint","localhost:4566")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
                .config("spark.hadoop.fs.s3a.path.style.access","true")
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.change.detection.mode","warn")
                .config("spark.hadoop.com.amazonaws.services.s3.enableV4","true")
                .config("spark.hadoop.fs.s3a.committer.name","partitioned")
                .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode","replace")
                .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
                .master("local[1]").getOrCreate();
        //Glow.register(spark);
        SparkSessionFactory sparkSessionFactory = new SparkSessionFactory(spark, Paths.get(".").toAbsolutePath().normalize().toString(), System.getProperty("java.io.tmpdir"), null, null, null);
        GorSession session = sparkSessionFactory.create();
        sparkGorSession = (GorSparkSession) session;
        pi = new SparkPipeInstance(session.getGorContext());
    }

    private void testSparkQuery(String query, String expectedResult) {
        PipeOptions pipeOptions = new PipeOptions();
        pipeOptions.query_$eq(query);
        pi.subProcessArguments(pipeOptions);
        String result = StreamSupport.stream(Spliterators.spliteratorUnknownSize(pi.theInputSource(), 0), false).map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong results from spark query: " + query, expectedResult, result);
    }

    @Test
    public void testSelectFromRedis() {
        testSparkQuery("select -p chr1 * from ../tests/data/gor/genes.gorz limit 5", "chr1\t11868\t14412\tDDX11L1\n" +
                "chr1\t14362\t29806\tWASH7P\n" +
                "chr1\t34553\t36081\tFAM138A\n" +
                "chr1\t53048\t54936\tAL627309.1\n" +
                "chr1\t62947\t63887\tOR4G11P");
    }

    @Test
    public void testSelectFromJson() throws IOException {
        var p = Paths.get("my.json");
        try {
            Files.writeString(p, "{\"ok\":\"simmi\"}");
            testSparkQuery("select * from my.json", "simmi");
        } finally {
            Files.delete(p);
        }
    }

    @Test
    public void testSelectFromJsonWithSchema() throws IOException {
        var p = Paths.get("my.json");
        try {
            Files.writeString(p, "{\"ok\":\"simmi\"}");
            testSparkQuery("select -schema {ok string} * from my.json", "simmi");
        } finally {
            Files.delete(p);
        }
    }

    @Test
    public void testSelectFromJsonWithFormat() throws IOException {
        var p = Paths.get("my.json");
        try {
            Files.writeString(p, "{\"ok\":\"simmi\"}");
            testSparkQuery("spark -format json -schema {ok string} my.json", "simmi");
        } finally {
            Files.delete(p);
        }
    }

    @Test
    @Ignore("Needs sqlite test file")
    public void testSelectFromSQLite() {
        testSparkQuery("select -format jdbc -option 'url=jdbc:sqlite:/Users/sigmar/create.db' * from simmi", "simmi");
    }

    @Test
    public void testGorzSparkSelectQuery() {
        testSparkQuery("select -p chr1 * from ../tests/data/gor/genes.gorz limit 5", "chr1\t11868\t14412\tDDX11L1\n" +
                "chr1\t14362\t29806\tWASH7P\n" +
                "chr1\t34553\t36081\tFAM138A\n" +
                "chr1\t53048\t54936\tAL627309.1\n" +
                "chr1\t62947\t63887\tOR4G11P");
    }

    @Test
    public void testGorzSparkSQLQuery() {
        testSparkQuery("spark select * from ../tests/data/gor/genes.gorz limit 5", "chr1\t11868\t14412\tDDX11L1\n" +
                "chr1\t14362\t29806\tWASH7P\n" +
                "chr1\t34553\t36081\tFAM138A\n" +
                "chr1\t53048\t54936\tAL627309.1\n" +
                "chr1\t62947\t63887\tOR4G11P");
    }

    @Test
    @Ignore("Test with localstack")
    public void testGorzSparkSQLQueryRemoteSource() {
        testSparkQuery("select * from s3a://my-bucket/test.gorz limit 5", "chr1\t11868\t14412\tDDX11L1\n" +
                "chr1\t14362\t29806\tWASH7P\n" +
                "chr1\t34553\t36081\tFAM138A\n" +
                "chr1\t53048\t54936\tAL627309.1\n" +
                "chr1\t62947\t63887\tOR4G11P");
    }

    @Test
    public void testInnerSparkSQLWithNestedNor() {
        testSparkQuery("spark select * from ../tests/data/gor/genes.gor where gene_symbol in (select gene_symbol from <(nor ../tests/data/gor/genes.gor | grep 'BRCA' | select gene_symbol))",
                "chr17\t41196311\t41322290\tBRCA1\n" +
                        "chr13\t32889610\t32973805\tBRCA2");
    }

    @Test
    public void testSparkSQLWithHint() {
        testSparkQuery("select /*+ BROADCAST(b) */ a.* from ../tests/data/gor/genes.gor a join (select * from ../tests/data/gor/genes.gor where gene_symbol like 'BRCA%') b where a.gene_symbol = b.gene_symbol",
                        "chr13\t32889610\t32973805\tBRCA2\n" +
                "chr17\t41196311\t41322290\tBRCA1");
    }

    @Test
    public void testSparkSQLWithNestedNor() {
        testSparkQuery("spark select gene_symbol from <(nor ../tests/data/gor/genes.gor | grep 'BRCA' | select gene_symbol)",
                "BRCA2\nBRCA1");
    }

    @Test
    public void testSparkSQLCreateWithError() {
        try {
            testSparkQuery("create xxx = select * from ../tests/data/gor/genes.gor | throwif posof(gene_symbol,'BRCA')=0; nor [xxx] | top 2",
                    "chrN\t0\tBRCA2\nchrN\t0\tBRCA1");
            Assert.fail("Sould have failed");
        } catch(Exception e) {
            Assert.assertEquals("Wrong exception message", "Gor throw on: posof(gene_symbol,'BRCA')=0", e.getCause().getCause().getCause().getCause().getCause().getMessage());
        }
    }

    @Test
    public void testSparkSQLWithNestedNorFile() throws IOException {
        String twocolNor = "#stuff\tgene_symbol\na\tBRCA2\nb\tBRCA1\n";
        Path tmpfile = Files.createTempFile("test", ".tsv");
        Files.write(tmpfile, twocolNor.getBytes());
        testSparkQuery("spark select gene_symbol from <(nor " + tmpfile.toString() + ")",
                "BRCA2\n" +
                        "BRCA1");
        Files.delete(tmpfile);
    }

    @Test
    public void testEmptySplit() {
        var res = sparkGorSession.dataframe("pgor -split <(gor -p chrA ../tests/data/gor/genes.gorz) ../tests/data/gor/genes.gorz", null);
        var sres = res.collectAsList().stream().map(Row::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong result","",sres);
    }

    @Test
    public void testGorSparkSQLQuery() {
        testSparkQuery("spark select * from ../tests/data/gor/genes.gor limit 5", "chr1\t11868\t14412\tDDX11L1\n" +
                "chr1\t14362\t29806\tWASH7P\n" +
                "chr1\t34553\t36081\tFAM138A\n" +
                "chr1\t53048\t54936\tAL627309.1\n" +
                "chr1\t62947\t63887\tOR4G11P");
    }

    @Test
    public void testParquetSparkSQLQuery() {
        testSparkQuery("spark select * from ../tests/data/parquet/dbsnp_test.parquet limit 5", "chr1\t10179\tC\tCC\trs367896724\n" +
                "chr1\t10250\tA\tC\trs199706086\n" +
                "chr10\t60803\tT\tG\trs536478188\n" +
                "chr10\t61023\tC\tG\trs370414480\n" +
                "chr11\t61248\tG\tA\trs367559610");
    }

    @Test
    public void testGorzSparkQuery() {
        testSparkQuery("spark ../tests/data/gor/genes.gorz | top 5", "chr1\t11868\t14412\tDDX11L1\n" +
                "chr1\t14362\t29806\tWASH7P\n" +
                "chr1\t34553\t36081\tFAM138A\n" +
                "chr1\t53048\t54936\tAL627309.1\n" +
                "chr1\t62947\t63887\tOR4G11P");
    }

    @Test
    public void testGorSparkQuery() {
        testSparkQuery("spark ../tests/data/gor/genes.gor | top 5", "chr1\t11868\t14412\tDDX11L1\n" +
                "chr1\t14362\t29806\tWASH7P\n" +
                "chr1\t34553\t36081\tFAM138A\n" +
                "chr1\t53048\t54936\tAL627309.1\n" +
                "chr1\t62947\t63887\tOR4G11P");
    }

    @Test
    public void testParquetSparkQuery() {
        testSparkQuery("spark ../tests/data/parquet/dbsnp_test.parquet | top 5", "chr1\t10179\tC\tCC\trs367896724\n" +
                "chr1\t10250\tA\tC\trs199706086\n" +
                "chr10\t60803\tT\tG\trs536478188\n" +
                "chr10\t61023\tC\tG\trs370414480\n" +
                "chr11\t61248\tG\tA\trs367559610");
    }

    @Test
    public void testCreateSparkQuery() {
        testSparkQuery("create xxx = spark ../tests/data/parquet/dbsnp_test.parquet | top 5; gor [xxx]", "chr1\t10179\tC\tCC\trs367896724\n" +
                "chr1\t10250\tA\tC\trs199706086\n" +
                "chr10\t60803\tT\tG\trs536478188\n" +
                "chr10\t61023\tC\tG\trs370414480\n" +
                "chr11\t61248\tG\tA\trs367559610");
    }

    @Test
    public void testCreateSparkQueryWithWrite() {
        testSparkQuery("create xxx = spark ../tests/data/parquet/dbsnp_test.parquet | top 5 | write -d test.gorz; gor [xxx]/dict.gord", "chr1\t10179\tC\tCC\trs367896724\n" +
                "chr1\t10250\tA\tC\trs199706086\n" +
                "chr10\t60803\tT\tG\trs536478188\n" +
                "chr10\t61023\tC\tG\trs370414480\n" +
                "chr11\t61248\tG\tA\trs367559610");
    }

    @Test
    @Ignore("Test with localstack")
    public void testCreateSparkQueryWithWriteToRemote() {
        testSparkQuery("create xxx = spark ../tests/data/parquet/dbsnp_test.parquet | top 5 | write -d s3a://my-bucket/test.gorz dict.gord; gor [xxx]", "chr1\t10179\tC\tCC\trs367896724\n" +
                "chr1\t10250\tA\tC\trs199706086\n" +
                "chr10\t60803\tT\tG\trs536478188\n" +
                "chr10\t61023\tC\tG\trs370414480\n" +
                "chr11\t61248\tG\tA\trs367559610");
    }

    @Test
    public void testCreateGorSparkQuery() {
        testSparkQuery("create xxx = gor ../tests/data/parquet/dbsnp_test.parquet | top 2; create yyy = spark ../tests/data/parquet/dbsnp_test.parquet | top 3; gor [xxx] [yyy]", "chr1\t10179\tC\tCC\trs367896724\n" +
                "chr1\t10179\tC\tCC\trs367896724\n" +
                "chr1\t10250\tA\tC\trs199706086\n" +
                "chr1\t10250\tA\tC\trs199706086\n" +
                "chr10\t60803\tT\tG\trs536478188");
    }

    @Test
    public void testGorzSparkQueryWithCalcWhere() {
        testSparkQuery("spark ../tests/data/gor/genes.gorz | top 5 | calc a 'a' | where Gene_Symbol = 'WASH7P'", "chr1\t14362\t29806\tWASH7P\ta");
    }

    @Test
    public void testGorzSparkQueryWithGorpipe() {
        testSparkQuery("spark ../tests/data/gor/genes.gorz | top 5 | group chrom -count", "chr1\t0\t250000000\t5");
    }

    @Test
    @Ignore("Test freeze")
    public void testExternalCommand() throws IOException {
        String pycode = "#!/usr/bin/env python\n" +
                "import sys\n" +
                "line = sys.stdin.readline()\n" +
                "sys.stdout.write( line )\n" +
                "sys.stdout.flush()\n" +
                "line = sys.stdin.readline()\n" +
                "while line:\n" +
                "    sys.stdout.write( line )\n" +
                "    line = sys.stdin.readline()\n" +
                "sys.stdout.flush()\n";
        Path pyscript = Paths.get("pass.py");
        Files.writeString(pyscript, pycode);
        testSparkQuery("spark ../tests/data/gor/genes.gorz | top 100 | cmd {python pass.py} | group chrom -count", "chr1\t0\t250000000\t100");
    }

    @After
    public void close() {
        if (pi != null) pi.close();
    }
}
