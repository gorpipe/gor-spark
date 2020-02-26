package gorsat.Spark

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

object GorOptimizationRules {

  def registration : SparkSessionExtensions => Unit = { ext => ext.injectOptimizerRule(GorOptimizationRule) }

}

case class GorOptimizationRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case l @ LocalLimit(x1 : Expression, _) => {
        val source = l.find( p => p.isInstanceOf[DataSourceV2Relation] )
        var result:LogicalPlan = l
        if(source.isDefined) {
          result = l transform {
            case d @ DataSourceV2Relation(_,_, d3, _,_) =>
              d.copy(options =  d3 ++ Map("top" -> x1.toString()))
          }
        }
        println(result)
        result
      }
      case l @ Aggregate(groupingExpressions, aggregateExpressions, child ) => {
        val result:LogicalPlan = aggregateExpressions.head match {
          case Alias(AggregateExpression(Count(Literal(1,_)::Nil), _,_,_), name) => {
            val source = l.find( p => p.isInstanceOf[DataSourceV2Relation] )
            var result:LogicalPlan = l
            if(source.isDefined) {
              result = l transform {
                case d @ DataSourceV2Relation(_,_, d3, _,_) =>
                  d.copy(options =  d3 ++ Map("count" -> "true"))
              }
            }
            println(result)
            result
          }
          case other => {
            l
          }
        }
        result
      }
      case default => {
        default
      }
    }
  }
}