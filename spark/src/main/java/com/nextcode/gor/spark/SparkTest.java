package com.nextcode.gor.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        //sparkConf.set("spark.submit.deployMode","client");
        //sparkConf.set("spark.home","/Users/sigmar/spark");
        SparkSession spark = /*SparkGorUtilities.getSparkSession("/gorproject","");//*/SparkSession.builder().master("local[*]").config(sparkConf).getOrCreate();
        System.err.println(spark.conf().getAll());
        Dataset<Row> ds = spark.read().format("csv").option("header","true").option("delimiter","\t").option("inferSchema","true").load("/gorproject/ref/dbsnp/dbsnp.gor");
        ds.createOrReplaceTempView("dbsnp");
        Dataset<Row> sqlds = spark.sql("select * from dbsnp where rsids = 'rs22'");
        sqlds.write().save("/gorproject/mu.parquet");
        //System.err.println(sqlds.count());
        spark.close();
    }
}
