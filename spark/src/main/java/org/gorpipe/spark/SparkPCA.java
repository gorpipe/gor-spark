package org.gorpipe.spark;

import breeze.linalg.DenseMatrix;
import com.google.common.collect.Iterators;
import gorsat.process.GorPipe;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SparkPCA {
    static String[] testargs = {"--projectroot","/gorproject","--freeze","plink_wes","--variants","/gorproject/testvars2.gor","--pnlist","/gorproject/testpns.txt","--partsize","4","--pcacomponents","3","--outfile","/gorproject/out.txt"};

    public static void main(String[] args) throws IOException {
        args = testargs;
        List<String> argList = Arrays.asList(args);
        int i = argList.indexOf("--appname");
        String appName = i != -1 ? argList.get(i+1) : "pca";
        i = argList.indexOf("--freeze");
        String freeze = i != -1 ? argList.get(i+1) : null;
        i = argList.indexOf("--projectroot");
        String projectRoot = i != -1 ? argList.get(i+1) : null;
        i = argList.indexOf("--variants");
        String variants = i != -1 ? argList.get(i+1) : null;
        i = argList.indexOf("--pnlist");
        String pnlist = i != -1 ? argList.get(i+1) : null;
        i = argList.indexOf("--partsize");
        int partsize = i != -1 ? Integer.parseInt(argList.get(i+1)) : 10;
        i = argList.indexOf("--pcacomponents");
        int pcacomponents = i != -1 ? Integer.parseInt(argList.get(i+1)) : 3;
        i = argList.indexOf("--outfile");
        String outfile = i != -1 ? argList.get(i+1) : null;
        try(SparkSession spark = SparkSession.builder()/*.master("local[*]")*/.appName(appName).getOrCreate()) {
            pca(spark, projectRoot, freeze, pnlist, variants, partsize, pcacomponents, outfile);
        }
    }

    private static void pca(SparkSession spark, String projectRoot, String freeze, String pnlist, String variants, int partsize, int pcacomponents, String outfile) throws IOException {
        GorSparkSession gorSparkSession = SparkGOR.createSession(spark, projectRoot, "result_cache", 0);
        Path root = Paths.get(projectRoot);
        Path freezepath = root.resolve(freeze);
        Dataset<? extends Row> dscount = gorSparkSession.spark("spark " + variants,null);
        int varcount = (int)dscount.count();

        String freezevariants = freezepath.resolve("variants.gord").toString();
        Dataset<Row> ds = (Dataset<Row>)gorSparkSession.spark("spark <(partgor -ff "+pnlist+" -partsize "+partsize+" -dict "+freezevariants+" <(gor "+variants +
                        "| varjoin -r -l -e '?' <(gor "+freezevariants+" -nf -f #{tags})" +
                        "| rename Chrom CHROM | rename ref REF | rename alt ALT " +
                        "| calc ID chrom+'_'+pos+'_'+ref+'_'+alt " +
                        "| csvsel "+freezepath.resolve("buckets.tsv").toString()+" <(nor <(gorrow 1,1 | calc pn '#{tags}' | split pn) | select pn) -u 3 -gc id,ref,alt -vs 1 | replace values 'u'+values))"
                , null);

        JavaRDD<Tuple2<Tuple2<Object,Object>,Matrix>> dbm = ds.select("values").javaRDD().mapPartitionsWithIndex((Function2<Integer, Iterator<Row>, Iterator<Tuple2<Tuple2<Object, Object>, Matrix>>>) (pi, input) -> {
            double[] mat = null;
            Iterator<Tuple2<Tuple2<Object,Object>,Matrix>> it = Collections.emptyIterator();
            int start = 0;
            while(input.hasNext()) {
                Row row = input.next();
                String strvec = row.getString(0).substring(1);
                int len = strvec.length();
                if(mat==null) {
                    mat = new double[varcount*len];
                }
                for(int i = 0; i < len; i++) {
                    mat[start+i] = strvec.charAt(i)-'0';
                }
                start += len;
            }
            if(mat!=null) {
                Matrix matrix = Matrices.dense(mat.length/varcount,varcount,mat);
                Tuple2<Object,Object> index = new Tuple2<>(0,10*pi);
                Tuple2<Tuple2<Object,Object>,Matrix> tupmat = new Tuple2<>(index,matrix);
                return Iterators.singletonIterator(tupmat);
            }
            return it;
        },true);

        BlockMatrix mat = new BlockMatrix(dbm.rdd(),varcount,10);
        RowMatrix rowMatrix = mat.transpose().toIndexedRowMatrix().toRowMatrix();
        Matrix pc = rowMatrix.computePrincipalComponents( pcacomponents);

        // Project the rows to the linear space spanned by the top 4 principal components.
        RowMatrix projected = rowMatrix.multiply(pc);

        DenseMatrix dm = projected.toBreeze();

        Path pnpath = Paths.get(pnlist);
        List<String> pns = Files.lines(pnpath).skip(1).collect(Collectors.toList());

        Path outpath = Paths.get(outfile);
        try (BufferedWriter bw = Files.newBufferedWriter(outpath)) {
            bw.write("#PN\tcol1\tcol2\tcol3\n");
            for (int i = 0; i < pns.size(); i++) {
                bw.write(pns.get(i));
                for (int k = 0; k < pcacomponents; k++) {
                    bw.write("\t" + dm.apply(i, k));
                }
                bw.write("\n");
            }
        }
    }
}
