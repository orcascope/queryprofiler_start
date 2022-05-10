package queryMonitor

import java.util.UUID

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
//FixedPoint


class Regularizer extends RuleExecutor[LogicalPlan]{
  override protected def batches: Seq[Batch] = Seq(Batch("My first Rule", FixedPoint(1), RegularizeExprOrders))

  object RegularizeExprOrders extends Rule[LogicalPlan]{

    val fixedExprId = ExprId(0L, UUID.fromString("6d37d815-ceea-4ae0-a051-b366f55b0e88"))

    override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      case p @ Project(projList, _) =>
        val newProjList = projList.map {
          case a: Alias => a
          case ne => Alias(ne, "none")(exprId = fixedExprId)
        }
        p.copy(projectList = newProjList)

      case a @ Aggregate(_, aggExprs, _) =>
        val newAggExprs = aggExprs.map {
          case a: Alias => {
            println("a was hit "+a)
            a
          }
          case ne => println("ne was hit "+ne)
            Alias(ne, "none")(exprId = fixedExprId)
        }
        a.copy(aggregateExpressions = newAggExprs)
    }
  }
}
