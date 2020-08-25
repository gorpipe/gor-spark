package org.gorpipe.spark

import org.apache.spark.sql.types.StructType
import org.gorpipe.gor.function.{GorRowFilterFunction, GorRowMapFunction}

class SparkUtils {
  def where(w: String, schema: StructType): GorSparkRowFilterFunction[org.gorpipe.gor.model.Row] = {
    new GorSparkRowFilterFunction[org.gorpipe.gor.model.Row](w, schema)
  }

  def where(w: String, header: Array[String], gortypes: Array[String]): GorRowFilterFunction[org.gorpipe.gor.model.Row] = {
    new GorRowFilterFunction[org.gorpipe.gor.model.Row](w, header, gortypes)
  }

  def calc(c: String, header: Array[String], gortypes: Array[String]): GorRowMapFunction = {
    new GorRowMapFunction(c, header, gortypes)
  }
}
