package org.gorpipe.spark;

import gorsat.process.SparkPipeInstance;
import org.apache.spark.sql.SparkSession;
import org.gorpipe.gor.session.GorSession;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class UTestGorDictionaryFolder {
    SparkSession spark;
    GorSparkSession sparkGorSession;
    SparkPipeInstance pi;

    public void deleteFolder(Path folderpath) {
        try {
            Files.walk(folderpath).sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    // ignore
                }
            });
        } catch (IOException e) {
            // ignore
        }
    }

    @Before
    public void init() {
        System.setProperty("org.gorpipe.gor.driver.gord.folders","true");
        spark = SparkSession.builder().master("local[1]").getOrCreate();
        //Glow.register(spark);
        SparkSessionFactory sparkSessionFactory = new SparkSessionFactory(spark, Paths.get(".").toAbsolutePath().normalize().toString(), System.getProperty("java.io.tmpdir"), null, null,null, null);
        GorSession session = sparkSessionFactory.create();
        sparkGorSession = (GorSparkSession) session;
        pi = new SparkPipeInstance(session.getGorContext());
    }

    @After
    public void close() {
        System.setProperty("org.gorpipe.gor.driver.gord.folders","false");
        if (pi != null) pi.close();
    }

    @Test
    public void testWriteToFolder() {
        Path folderpath = Paths.get("folder1.gord");
        try {
            TestUtils.runGorPipe(pi, "gor -p chr21 ../tests/data/gor/genes.gor | write -d "+folderpath);
            TestUtils.runGorPipe(pi,"gor -p chr22 ../tests/data/gor/genes.gor | write -d "+folderpath);
            String results = TestUtils.runGorPipe(pi,"gor "+folderpath+" | group chrom -count");
            Assert.assertEquals("Wrong results in write folder", "Chrom\tbpStart\tbpStop\tallCount\n" +
                    "chr21\t0\t100000000\t669\n" +
                    "chr22\t0\t100000000\t1127\n", results);
        } finally {
            deleteFolder(folderpath);
        }
    }

    @Test
    public void testCreateWriteFolder() {
        Path folderpath = Paths.get("folder2.gord");
        try {
            String results = TestUtils.runGorPipe(pi,"create a = gor -p chr21 ../tests/data/gor/genes.gor | write " + folderpath +
                    "; create b = gor -p chr22 ../tests/data/gor/genes.gor | write " + folderpath +
                    "; gor " + folderpath + " | group chrom -count");
            Assert.assertEquals("Wrong results in write folder", "Chrom\tbpStart\tbpStop\tallCount\n" +
                    "chr21\t0\t100000000\t669\n" +
                    "chr22\t0\t100000000\t1127\n", results);
        } finally {
            deleteFolder(folderpath);
        }
    }

    @Test
    public void testWritePassThrough() {
        Path path = Paths.get("gorfile.gorz");
        String results = TestUtils.runGorPipe(pi,"gor ../tests/data/gor/genes.gor | top 1 | write -p " + path);
        Assert.assertEquals("Wrong results in write folder", "Chrom\tgene_start\tgene_end\tGene_Symbol\n" +
                "chr1\t11868\t14412\tDDX11L1\n" , results);
        try {
            Files.delete(path);
        } catch (IOException e) {
            // Ignore
        }
    }

    @Test
    public void testCreateWrite() {
        Path path = Paths.get("gorfile.gorz");
        String results = TestUtils.runGorPipe(pi,"create a = gor -p chr21 ../tests/data/gor/genes.gor | write " + path + "; gor [a] | group chrom -count");
        Assert.assertEquals("Wrong results in write folder", "Chrom\tbpStart\tbpStop\tallCount\n" +
                "chr21\t0\t100000000\t669\n" , results);
        try {
            Files.delete(path);
        } catch (IOException e) {
            // Ignore
        }
    }

    @Test
    public void testGorCardinalityColumn() throws IOException {
        Path path = Paths.get("gorfile.gorz");
        TestUtils.runGorPipe(pi,"gor -p chr21 ../tests/data/gor/genes.gor | calc c substr(gene_symbol,0,1) | write -card c " + path);
        Path metapath = Paths.get("gorfile.gorz.meta");
        String metainfo = Files.readString(metapath);
        Assert.assertEquals("Wrong results in meta file", "## SERIAL = 0\n" +
                        "## LINE_COUNT = 669\n" +
                        "## TAGS = \n" +
                        "## MD5 = 162498408aa03202fa1d2327b2cf9c4f\n" +
                        "## RANGE = chr21\t9683190\tchr21\t48110675\n" +
                        "## CARDCOL = [c]: A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,R,S,T,U,V,W,Y,Z\n",
                metainfo);
        try {
            Files.delete(path);
        } catch (IOException e) {
            // Ignore
        }
    }

    @Test
    @Ignore("Same tests in gor")
    public void testPgorWriteFolder() {
        Path folderpath = Paths.get("folder.gord");
        try {
            String results = TestUtils.runGorPipe(pi,"create a = pgor ../tests/data/gor/genes.gor | write -d " + folderpath +
                    "; gor [a] | group chrom -count");
            //"; gor [a] | group chrom -count");
            Assert.assertEquals("Wrong results in write folder", "Chrom\tbpStart\tbpStop\tallCount\n" +
                    "chr1\t0\t250000000\t4747\n" +
                    "chr10\t0\t150000000\t2011\n" +
                    "chr11\t0\t150000000\t2982\n" +
                    "chr12\t0\t150000000\t2524\n" +
                    "chr13\t0\t150000000\t1165\n" +
                    "chr14\t0\t150000000\t2032\n" +
                    "chr15\t0\t150000000\t1864\n" +
                    "chr16\t0\t100000000\t2161\n" +
                    "chr17\t0\t100000000\t2659\n" +
                    "chr18\t0\t100000000\t992\n" +
                    "chr19\t0\t100000000\t2748\n" +
                    "chr2\t0\t250000000\t3507\n" +
                    "chr20\t0\t100000000\t1183\n" +
                    "chr21\t0\t100000000\t669\n" +
                    "chr22\t0\t100000000\t1127\n" +
                    "chr3\t0\t200000000\t2648\n" +
                    "chr4\t0\t200000000\t2245\n" +
                    "chr5\t0\t200000000\t2523\n" +
                    "chr6\t0\t200000000\t2557\n" +
                    "chr7\t0\t200000000\t2514\n" +
                    "chr8\t0\t150000000\t2107\n" +
                    "chr9\t0\t150000000\t2156\n" +
                    "chrM\t0\t20000\t37\n" +
                    "chrX\t0\t200000000\t2138\n" +
                    "chrY\t0\t100000000\t480\n", results);
        } finally {
            deleteFolder(folderpath);
        }
    }

    @Test
    @Ignore("Same tests in gor")
    public void testPgorWriteFolderWithCardinality() throws IOException {
        Path folderpath = Paths.get("folder.gord");
        try {
            TestUtils.runGorPipe(pi,"create a = pgor ../tests/data/gor/genes.gor | where chrom = 'chrM' | calc c substr(gene_symbol,0,1) | write -card c -d " + folderpath +
                    "; gor [a] | group chrom -count");
            String thedict = Files.readString(folderpath.resolve("thedict.gord"));
            Assert.assertEquals("Wrong results in dictionary",
                    "#filepath\tbucket\tstartchrom\tstartpos\tendchrom\tendpos\tsource\n" +
                            "dd02aed74a26d4989a91f3619ac8dc20.gorz\t1\tchrM\t576\tchrM\t15955\tJ,M\n",
                    thedict);
        } finally {
            deleteFolder(folderpath);
        }
    }
}
