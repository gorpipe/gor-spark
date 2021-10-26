package gorsat.process;

//import is.hail.HailContext;
//import is.hail.methods.Skat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gorpipe.gor.model.GenomicIterator;
import org.gorpipe.gor.model.GenomicIteratorBase;
import org.gorpipe.gor.session.GorContext;
import org.gorpipe.gor.session.GorSession;
import org.gorpipe.model.gor.iterators.RowSource;
import org.gorpipe.spark.GorSparkRow;
import scala.Option;
import scala.collection.immutable.Set;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HailInputSource extends GenomicIteratorBase {
    //static HailContext hl;
    Dataset<Row> ds;
    Iterator<Row> it;
    String parquetPath;

    /*MatrixTable loadMatrixTable(String[] iargs) {
        MatrixTable mt = null;
        if(iargs[1].endsWith(".bed")) {
            //MatrixPLINKReader mpr = new MatrixPLINKReader(iargs[1],iargs[2],iargs[3], Option.empty(), "\\\\s+", "NA", false, true, Option.apply(ReferenceGenome.defaultReference()), Option.empty(), false);
            //mt = new MatrixTable(hl, mpr);
        } else if(iargs[1].endsWith(".bgen")) {
            Seq<String> seq1 =  new Set.Set1<>(iargs[1]).toSeq();
            //mt = hl.importBgens(seq1, Option.apply(iargs[2]), false, false, true, true, true, Option.empty(), Option.empty(), null, Option.empty());
        } else {
            mt = hl.readVDS(iargs[1], false, false);
        }
        return mt;
    }*/

    public HailInputSource(String[] iargs) {
        /*SparkSession sparkSession = SparkGorUtilities.getSparkSession("/gorproject", "/c/gorproject");
        if( hl == null ) hl = HailContext.getOrCreate(sparkSession.sparkContext(), "GOR", Option.empty(), "local[*]", "/tmp/hail.log", false, false, 10, 10, "/tmp", 0);

        Random r = new Random();
        if(iargs[0].equals("index")) {
            hl.indexBgen(Collections.singletonList(iargs[1]), null, Option.empty(), null, true);
        } else if(iargs[0].equals("regression")) {
            MatrixTable mt = loadMatrixTable(iargs);
            //TBoolean tb = TBoolean.apply(true);
            String[] pp = new String[] {"y"};
            scala.collection.immutable.List<String> p = JavaConversions.asScalaBuffer(Arrays.asList(pp)).toList();
            Object[] annotations = IntStream.range(0,mt.numCols()).mapToObj(i -> (double)Math.round(r.nextDouble())).toArray(Object[]::new);
            MatrixTable annomt = null;//mt.annotateCols(TFloat64.apply(true), p, annotations);

            String[] cov = new String[] {"cov"};
            scala.collection.immutable.List<String> pcov = JavaConversions.asScalaBuffer(Arrays.asList(cov)).toList();
            Object[] covannotations = IntStream.range(0,annomt.numCols()).mapToObj(i -> 1.0).toArray(Object[]::new);
            MatrixTable annomt2 = null;//annomt.annotateCols(TFloat64.apply(true), pcov, covannotations);

            ArrayList<String> covlist = new ArrayList<>(0);
            covlist.add("cov");
            /*Table lr = LogisticRegression.apply(annomt2, "wald", "y", "dosage", covlist, new ArrayList(0));
            if( lr.nKeys() > 0 ) {
                lr = lr.unkey();
            }
            ds = lr.expandTypes().flatten().toDF(sparkSession.sqlContext());*
            setHeader(String.join("\t",ds.columns()));
            it = ds.toLocalIterator();
            //lr.write(iargs[iargs.length-1],true,true,null);
        } else if(iargs[0].equals("skat")) {
            MatrixTable mt = loadMatrixTable(iargs);
            //is.hail.expr.types.virtual.Type type;
            //is.hail.expr.types.virtual.Type.
            String[] pp = new String[] {"y"};
            scala.collection.immutable.List<String> p = JavaConversions.asScalaBuffer(Arrays.asList(pp)).toList();
            Object[] annotations = IntStream.range(0,mt.numCols()).mapToObj(i -> (double)Math.round(r.nextDouble())).toArray(Object[]::new);
            MatrixTable annomt = null;//mt.annotateCols(TFloat64.apply(true), p, annotations);

            String[] cov = new String[] {"cov"};
            scala.collection.immutable.List<String> pcov = JavaConversions.asScalaBuffer(Arrays.asList(cov)).toList();
            Object[] covannotations = IntStream.range(0,annomt.numCols()).mapToObj(i -> 1.0).toArray(Object[]::new);
            MatrixTable annomt2 = null;//annomt.annotateCols(TFloat64.apply(true), pcov, covannotations);

            //mt.ann
            /*String[] weight = new String[] {"cweight"};
            scala.collection.immutable.List<String> pweight = JavaConversions.asScalaBuffer(Arrays.asList(weight)).toList();
            Object[] weightannotations = IntStream.range(0,annomt2.numCols()).mapToObj(i -> 1.0).toArray(Object[]::new);
            MatrixTable annomt3 = annomt2.annotateCols(TFloat64.apply(true), pweight, weightannotations);*

            List<Row> dList = IntStream.range(0,(int)mt.countRows()).mapToObj(f -> RowFactory.create(1.0)).collect(Collectors.toList());

            StructField[] sf = {new StructField("weight", DataTypes.DoubleType, true, Metadata.empty())};
            StructType st = new StructType(sf);
            Dataset<Row> dsr = sparkSession.createDataFrame(dList, st);
            System.err.println(String.join(" ", dsr.columns()));
            Table tb = null;//Table.fromDF(hl, dsr, new ArrayList<>());
            //MatrixTable jmt = tb.toMatrixTable(new String[]{"weight"}, new String[]{"cweight"}, new String[]{}, new String[]{}, Option.empty());
            ArrayList<String> irs = new ArrayList<>();
            irs.add("weight");
            MatrixTable annomt3 = null;//annomt2.annotateRowsTableIR(tb, "weight", irs);
            //annomt2.annotateRowsVDS(jmt, "weight");


            annomt3.write("/Users/sigmar/whataf",true,true,"");

            Skat skat = new Skat("locus","weight","y","dosage",new String[] {"cov"},true,10,1.0,10);
            skat.execute(hl.exe);
            Table result = Skat.apply(annomt3,"locus","weight","y","dosage",new String[] {"cov"},true,10,1.0,10);
            //ds = result.expandTypes().toDF(sparkSession.sqlContext());
            setHeader(String.join("\t",ds.columns()));
            it = ds.toLocalIterator();
        } else if(iargs[0].equals("gor")) {
            MatrixTable mt = loadMatrixTable(iargs);
            /*Table rowsTable = mt.rowsTable();
            if( rowsTable.nKeys() > 0 ) {
                rowsTable = rowsTable.unkey();
            }
            ds = rowsTable.expandTypes().flatten().toDF(sparkSession.sqlContext());*
            setHeader(String.join("\t",ds.columns()));
            it = ds.toLocalIterator();
        }*/
    }

    public void write(String parquetPath) {
        it = null;
        this.parquetPath = parquetPath;
    }

    @Override
    public boolean seek(String chr, int pos) {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        if( it != null ) return it.hasNext();
        else {
            ds.write().parquet(parquetPath);
            return false;
        }
    }

    @Override
    public org.gorpipe.gor.model.Row next() {
        return new GorSparkRow(it.next());
    }
}
