package org.gorpipe.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.sources.Filter

import scala.jdk.CollectionConverters._

object ScalaUtils {
  def seqAttribute(seq: Seq[AttributeReference]): Seq[Attribute] = {
    seq.map(a => a.toAttribute)
  }

  def filterSeq(filters: Seq[Filter]): java.util.List[Filter] = {
    filters.asJava
  }

  def iterator(it: java.util.Iterator[InternalRow]): Iterator[InternalRow] = {
    it.asScala
  }

  def iteratorJava(it: Iterator[InternalRow]): java.util.Iterator[InternalRow] = {
    it.asJava
  }

  def columns(colNames: Array[String]): Seq[Column] = {
    colNames.map(c => org.apache.spark.sql.functions.col(c)).toSeq
  }

  def toSeq(list: Array[String]): Seq[String] = {
    list.toSeq
  }
}
