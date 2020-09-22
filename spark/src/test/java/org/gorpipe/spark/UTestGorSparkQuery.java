package org.gorpipe.spark;

import gorsat.process.PipeInstance;
import gorsat.process.PipeOptions;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.session.GorSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class UTestGorSparkQuery {

    PipeInstance pi;

    @Before
    public void init() {
        SparkSession sparkSession = SparkSession.builder().master("local[1]").getOrCreate();
        //Glow.register(sparkSession);
        SparkSessionFactory sparkSessionFactory = new SparkSessionFactory(sparkSession, Paths.get(".").toAbsolutePath().normalize().toString(), "/tmp", null);
        GorSession session = sparkSessionFactory.create();
        pi = new PipeInstance(session.getGorContext());
    }

    private void testSparkQuery(String query, String expectedResult) {
        PipeOptions pipeOptions = new PipeOptions();
        pipeOptions.query_$eq(query);
        pi.subProcessArguments(pipeOptions);
        String result = StreamSupport.stream(Spliterators.spliteratorUnknownSize(pi.theInputSource(), 0), false).map(Object::toString).collect(Collectors.joining("\n"));
        Assert.assertEquals("Wrong results from spark query: " + query, expectedResult, result);
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
    public void testSparkSQLWithNestedNorFile() throws IOException {
        String twocolNor = "#stuff\tgene_symbol\na\tBRCA2\nb\tBRCA1\n";
        Path tmpfile = Files.createTempFile("test", "tsv");
        Files.write(tmpfile, twocolNor.getBytes());
        testSparkQuery("spark select gene_symbol from <(nor " + tmpfile.toString() + ")",
                "BRCA2\n" +
                        "BRCA1");
        Files.delete(tmpfile);
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
