package gorsat.Spark

import org.apache.spark.sql.sources.{And, EqualTo, Filter, Or}

object GorFilterHandler {

  def findAll(filter: Filter) : Seq[EqualTo] = {
    filter match {
      case And(left, right) => Seq(left,right).flatMap(findAll)
      case Or(left, right) => Seq(left,right).flatMap(findAll)
      case e: EqualTo => Seq(e)
      case _ => Nil
    }
  }

}
