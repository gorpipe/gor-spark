package org.gorpipe.spark;

import gorsat.process.PipeOptions;
import gorsat.process.SparkPipeInstance;
import io.projectglow.Glow;
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

public class UTestSparkPCA {
    SparkSession spark;
    SparkPipeInstance pi;

    @Before
    public void init() {
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        Glow.register(spark, false);
        SparkSessionFactory sparkSessionFactory = new SparkSessionFactory(spark, Paths.get(".").toAbsolutePath().normalize().toString(), System.getProperty("java.io.tmpdir"), null, null, null);
        GorSession session = sparkSessionFactory.create();
        pi = new SparkPipeInstance(session.getGorContext());
    }

    @After
    public void close() {
        if (pi != null) pi.close();
        if (spark != null) spark.close();
    }

    private void testSparkQuery(String query, String expectedResult) {
        PipeOptions pipeOptions = new PipeOptions();
        pipeOptions.query_$eq(query);
        pi.subProcessArguments(pipeOptions);
        String content = StreamSupport.stream(Spliterators.spliteratorUnknownSize(pi.theInputSource(), 0), false).map(Object::toString).collect(Collectors.joining("\n"));
        String result = pi.getHeader() + "\n" + content;
        Assert.assertEquals("Wrong results from spark query: " + query, expectedResult, result);
    }

    @Test
    @Ignore("Not ready")
    public void testSparkPCAModelWrite() throws IOException {
        Path bucketFile = Paths.get("buckets.tsv");
        Path variantBucketFile1 = Paths.get("variants1.gor");
        Path variantBucketFile2 = Paths.get("variants2.gor");
        Path variantBucketFile3 = Paths.get("variants3.gor");
        Path variantDictFile = Paths.get("variants.gord");
        Path pnpath = Paths.get("pns.txt");
        int partsize = 4;
        Files.writeString(pnpath,"a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\n");
        Files.writeString(bucketFile, "a\t1\nb\t1\nc\t1\nd\t1\ne\t2\nf\t2\ng\t2\nh\t2\ni\t3\nj\t3\nk\t3\nl\t3\n");
        Files.writeString(variantBucketFile1,"Chrom\tpos\tref\talt\tbucket\tvalues\n"+
                "chr1\t1\tA\tC\t1\t0011\n"+
                "chr1\t2\tG\tC\t1\t0201\n"+
                "chr1\t3\tA\tC\t1\t0011\n"+
                "chr1\t4\tG\tC\t1\t0201\n");
        Files.writeString(variantBucketFile2,"Chrom\tpos\tref\talt\tbucket\tvalues\n"+
                "chr1\t1\tA\tC\t2\t0102\n"+
                "chr1\t2\tG\tC\t2\t0221\n"+
                "chr1\t3\tA\tC\t2\t0102\n"+
                "chr1\t4\tG\tC\t2\t0221\n");
        Files.writeString(variantBucketFile3,"Chrom\tpos\tref\talt\tbucket\tvalues\n"+
                "chr1\t1\tA\tC\t3\t0122\n"+
                "chr1\t2\tG\tC\t3\t1201\n"+
                "chr1\t3\tA\tC\t3\t1122\n"+
                "chr1\t4\tG\tC\t3\t0001\n");
        Files.writeString(variantDictFile,"variants1.gor\t1\tchr1\t0\tchrZ\t1000000000\ta,b,c,d\n"+
                "variants2.gor\t2\tchr1\t0\tchrZ\t1000000000\te,f,g,h\n"+
                "variants3.gor\t3\tchr1\t0\tchrZ\t1000000000\ti,j,k,l\n");

        testSparkQuery(
                "create xxx = spark <(partgor -ff "+pnpath+" -partsize "+partsize+" -dict "+variantDictFile+" <(gor "+variantBucketFile1 +
                "| select 1,2,3,4 | varjoin -r -l -e '?' <(gor "+variantDictFile+" -nf -f #{tags})" +
                "| rename Chrom CHROM | rename ref REF | rename alt ALT " +
                "| calc ID chrom+'_'+pos+'_'+ref+'_'+alt " +
                "| csvsel "+bucketFile+" <(nor <(gorrow 1,1 | calc pn '#{tags}' | split pn) | select pn) -u 3 -gc id,ref,alt -vs 1 | replace values 'u'+values)) | selectexpr values | gttranspose | calc norm_values normalize(values) | selectexpr norm_values as values | write -pca 2;" +

                "create yyy = spark -tag <(partgor -ff "+pnpath+" -partsize "+partsize+" -dict "+variantDictFile+" <(gor "+variantBucketFile1 +
                "| select 1,2,3,4 | varjoin -r -l -e '?' <(gor "+variantDictFile+" -nf -f #{tags})" +
                "| rename Chrom CHROM | rename ref REF | rename alt ALT " +
                "| calc ID chrom+'_'+pos+'_'+ref+'_'+alt " +
                "| csvsel "+bucketFile+" <(nor <(gorrow 1,1 | calc pn '#{tags}' | split pn) | select pn) -u 3 -gc id,ref,alt -vs 1 | replace values 'u'+values " +
                //"| calc pn '#{tags}'));" +
                ")) " +
                "| selectexpr values | gttranspose | calc norm_values normalize(values) | selectexpr norm_values as values | calc pca_result pcatransform([xxx]);" +
                "nor [yyy]", "");
    }
}
