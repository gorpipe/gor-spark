package org.gorpipe.spark;

import breeze.linalg.DenseMatrix;
import gorsat.process.GorPipe;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

public class SparkTest {
    public static SparkSession spark;

    public static void main(String[] args) throws IOException {
        //SparkPipe.main(args);

        SparkConf sparkConf = new SparkConf();
        //sparkConf.set("spark.submit.deployMode","client");
        //sparkConf.set("spark.home","/Users/sigmar/spark");
        //spark = SparkGorUtilities.getSparkSession("/gorproject","");
        spark = SparkSession.builder().master("local[*]").config(sparkConf).getOrCreate();
        //System.err.println(spark.conf().getAll());

        //test1(args);
        test2(args);
    }

    public static void test2(String[] args) throws IOException {
        GorSparkSession gorSparkSession = SparkGOR.createSession(spark);
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> ds = (Dataset<Row>)gorSparkSession.spark("spark -tag <(pgor -split <(gor /gorproject/brca.gor) /gorproject/plink_wes/metadata/AF.gorz | group chrom -count)",null);
        ds.collectAsList().forEach(System.err::println);

        //Dataset<? extends Row> ds = gorSparkSession.spark("spark <(pgor ref/genes.gorz | group chrom -count)",null);
        /*Dataset<Row> ds = (Dataset<Row>)gorSparkSession.spark("spark <(partgor -ff <(nor -h /gorproject/plink_wes/buckets.tsv | select 1 | top 500) -partsize 10 -dict /gorproject/plink_wes/variants.gord <(pgor -split <(gor /gorproject/brca.gor) /gorproject/plink_wes/variants.gord -nf -f #{tags} " +
                "| varjoin -r -l -e '?' /gorproject/plink_wes/vep_single.gorz" +
                "| where max_consequence in ('frameshift_variant','splice_acceptor_variant','splice_donor_variant','start_lost','stop_gained','stop_lost','incomplete_terminal_codon_variant','inframe_deletion','inframe_insertion','missense_variant','protein_altering_variant','splice_region_variant')" +
                "| rename Chrom CHROM | rename ref REF | rename alt ALT " +
                //"| varjoin -r -l -e '?' /gorproject/plink_wes/vep_single.gorz " +
                //"| where max_consequence in ('frameshift_variant','splice_acceptor_variant','splice_donor_variant','start_lost','stop_gained','stop_lost','incomplete_terminal_codon_variant','inframe_deletion','inframe_insertion','missense_variant','protein_altering_variant','splice_region_variant') "
                //"| varjoin -r -l -e 0.0 <(gor /gorproject/plink_wes/metadata/AF.gorz) " +
                //"| where isfloat(AF) and float(AF) <= 0.05 " +
                "| calc ID chrom+'_'+pos+'_'+ref+'_'+alt " +
                "| csvsel /gorproject/plink_wes/buckets.tsv <(nor -h /gorproject/plink_wes/buckets.tsv | select 1 | top 500) -u 3 -gc id,ref,alt -vs 1))"
                , null);

        //ds.collectAsList().forEach(System.err::println);
        System.err.println(ds.count());

        /*ArrayList<Map<String,Integer>> lmi = new ArrayList<>();
        Path p = Paths.get("buckets.tsv");
        Files.lines(p).map(l -> l.split("\t")).forEach(s -> {
            int i = Integer.parseInt(s[1]);
            lmi.ensureCapacity(i);
            Map<String,Integer> mi = lmi.get(i-1);
            if(mi==null) {
                mi = new HashMap<>();
                lmi.set(i-1,mi);
            }
            mi.put(s[0],mi.size());
        });

        Broadcast brdc = javaSparkContext.broadcast(javaSparkContext);

        Encoder<MatrixEntry> menc = Encoders.javaSerialization(MatrixEntry.class);
        Dataset<MatrixEntry> dsm = ds.map((MapFunction<Row, MatrixEntry>)  r -> {
            return new MatrixEntry(0,0,1.0):
        }, menc);
        CoordinateMatrix cm = new CoordinateMatrix(dsm.rdd());*

        Encoder<BlockMatrix> bmenc = Encoders.kryo(BlockMatrix.class);
        Encoder<Vector> enc = Encoders.kryo(Vector.class);
        /*Dataset<Vector> sparserows = ds.select("values").map((MapFunction<Row, Vector>) value -> {
            String strVector = value.getString(0);
            int[] ii = IntStream.range(0,strVector.length()).filter(i -> strVector.charAt(i)!='0').toArray();
            double[] dd = value.getString(0).chars().filter(c -> c != '0').asDoubleStream().toArray();
            return Vectors.sparse(ii.length, ii, dd);
        }, enc);*

        Dataset<BlockMatrix> dbm = ds.select("values").mapPartitions((MapPartitionsFunction<Row, BlockMatrix) input -> {
            List<double[]> dlist = new ArrayList<>();
            while(input.hasNext()) {
                String values = input.next().getString(0);
                double[] array = values.chars().asDoubleStream().toArray();
                dlist.add(array);
            }
            int rownum = dlist.get(0).length;
            double[] mat = new double[dlist.size()*rownum];
            for(int i = 0; i < dlist.size(); i++) System.arraycopy(dlist.get(i), 0, mat, i*rownum, rownum);
            Matrices.dense(rownum,dlist.size(),mat);
            //return new BlockMatrix();
        },bmenc);

        Dataset<Vector> denserows = ds.select("values").map((MapFunction<Row, Vector>) value -> Vectors.dense(value.getString(0).chars().asDoubleStream().toArray()), enc);
        JavaRDD<IndexedRow> rddi = denserows.javaRDD().zipWithIndex().map(t -> new IndexedRow(t._2, t._1));
        IndexedRowMatrix irm = new IndexedRowMatrix(rddi.rdd());
        CoordinateMatrix com = irm.toCoordinateMatrix();
        RowMatrix mat = com.transpose().toRowMatrix();

        // Compute the top 4 principal components.
        // Principal components are stored in a local dense matrix.
        Matrix pc = mat.computePrincipalComponents(3);

        // Project the rows to the linear space spanned by the top 4 principal components.
        RowMatrix projected = mat.multiply(pc);

        DenseMatrix dm = projected.toBreeze();
        System.err.println(dm.toString());
                //.collectAsList().forEach(System.err::println);*/
    }

    public static void test1(String[] args) {
        Dataset<Row> ds = spark.read().format("csv").option("header","true").option("delimiter","\t").option("inferSchema","true").load("/gorproject/ref/dbsnp/dbsnp.gor");
        ds.createOrReplaceTempView("dbsnp");
        Dataset<Row> sqlds = spark.sql("select * from dbsnp where rsids = 'rs22'");
        sqlds.write().save("/gorproject/mu.parquet");
        //System.err.println(sqlds.count());
        spark.close();
    }
}
