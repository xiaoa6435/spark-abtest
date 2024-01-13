/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.abtest.aggregate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.abtest.Factor
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.extra.FunctionDescription
import org.apache.spark.sql.types._

case class ClusterOLS(
  override val y: Expression,
  X: Seq[Expression],
  c: Expression,
  override val confLevel: Expression,
  override val stdErrorType: Expression,
  override val removeNaN: Expression,
  method: String,
  isFactor: Boolean
) extends HypothesisTest {
  def this(children: Seq[Expression], method: String) = {
    this(
      children.head,
      OLS.toX(children, method),
      children(OLS.offset(method)),
      children.applyOrElse(OLS.offset(method) + 1, (_: Int) => Literal(0.95)),
      children.applyOrElse(OLS.offset(method) + 2, (_: Int) => Literal("HC1")),
      children.applyOrElse(OLS.offset(method) + 3, (_: Int) => Literal(true)),
      method,
      Factor.isFactor(children, method)
    )
  }

  lazy val p: Int = X.length

  override def inputTypes: Seq[AbstractDataType] =
    (DoubleType +: Seq.fill(p)(DoubleType)) ++
      Seq(AnyDataType, DoubleType, StringType, BooleanType)

  override def dataType: DataType = OLS.dataTypes(method, isFactor)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }
    if (p < 1) {
      return TypeCheckFailure(s"The x must not empty, but got ${X.map(_.sql)}")
    }
    checkParaTypes()
  }

  override def prettyName: String = method

  override def children: Seq[Expression] =
    (y +: X) ++ Seq(c, confLevel, stdErrorType, removeNaN)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(y = newChildren(0), X = newChildren.slice(1, p + 1),
      c = newChildren(p + 1), confLevel = newChildren(p + 2),
      stdErrorType = newChildren(p + 3), removeNaN = newChildren(p + 4))

}

object ClusterOLS {
  val clusterOLSFD: FunctionDescription = OLS.fd("cluster_ols")
  val clusterTTestFD: FunctionDescription = OLS.fd("cluster_ttest")
  val clusterAnovaFD: FunctionDescription = OLS.fd("cluster_anova")
  val clusterAncova1FD: FunctionDescription = OLS.fd("cluster_ancova1")
  val clusterAncova2FD: FunctionDescription = OLS.fd("cluster_ancova2")

  private def rewrite(a: Aggregate): Aggregate = {
    val groupByMap = a.groupingExpressions.collect {
      case ne: NamedExpression => ne -> ne.toAttribute
      case e => e -> AttributeReference(e.sql, e.dataType, e.nullable)()
    }
    val groupByAttrs = groupByMap.map(_._2)

    val aggExpressions = a.aggregateExpressions.flatMap {
      _.collect { case ae: AggregateExpression => ae }
    }
    val clusterAggs = aggExpressions.filter {
      _.aggregateFunction.isInstanceOf[ClusterOLS]
    }
    // Extract cluster aggregate expressions.
    val clusterGroups = clusterAggs.groupBy {
      _.aggregateFunction.asInstanceOf[ClusterOLS].c
    }
    val clusterChildren = clusterGroups.values.flatten.toSeq.flatMap { ae =>
      val clusterOLS = ae.aggregateFunction.asInstanceOf[ClusterOLS]
      Seq(clusterOLS.y) ++ clusterOLS.X ++ Seq(clusterOLS.c) ++ ae.filter
    }.distinct
    val clusterChildrenAttrMap = clusterChildren.map { e =>
      e -> AttributeReference(e.sql, e.dataType, nullable = true)()
    }
    val clusterChildrenAttrLookup = clusterChildrenAttrMap.toMap
    val gid = AttributeReference("gid", IntegerType, nullable = false)()
    val (clusterProjections, _clusterAggOperatorMap) = clusterGroups.values.toSeq.zipWithIndex.map {
      case (expressions, i) =>
        val id = Literal(i + 1)
        val rawExprs = expressions.flatMap { ae =>
          val clusterOLS = ae.aggregateFunction.asInstanceOf[ClusterOLS]
          Seq(clusterOLS.y) ++ clusterOLS.X ++ Seq(clusterOLS.c) ++ ae.filter
        }.distinct
        val projections = clusterChildren.map {
          case e if rawExprs.contains(e) => e
          case e => nullify(e)
        } :+ id

        val operators = expressions.map { ae =>
          val clusterOLS@ClusterOLS(ye, xe, ce, confLevel, stdErrorType, removeNaN, method, nt) =
            ae.aggregateFunction
          val p = clusterOLS.p
          val y = clusterChildrenAttrLookup(ye)
          val X = xe.map(clusterChildrenAttrLookup)
          val yx = y +: X
          val suff1st = OLS.getSuff(yx, "const", removeNaN)
          val newCond = EqualTo(gid, id)
          val newFilter = ae.filter.flatMap(clusterChildrenAttrLookup.get)

          def toAggExpr1st(af: AggregateFunction) =
            af.toAggregateExpression(isDistinct = false, newFilter)

          val agg1st = CreateArray(suff1st.map(e => toAggExpr1st(Sum(e)))).as("agg1st")
          val suff2ndBase = Seq.range(0, OLS.suffSize(p, "const")).map { i =>
            GetArrayItem(agg1st.toAttribute, Literal(i))
          }
          val suff2ndXe = OLS.xeSuff(suff2ndBase, p)
          val suff2nd = suff2ndBase ++ suff2ndXe :+ Literal(1.0)

          def toAggExpr2nd(af: AggregateFunction) =
            af.toAggregateExpression(isDistinct = false, Some(newCond))

          val agg2nd = CreateArray(suff2nd.map(e => toAggExpr2nd(Sum(e))))
          val ols = OLSFromSuff(agg2nd, stdErrorType, p, method)
          val ret = OLSFromSuff.packOLSRet(
            ols, p, confLevel, method, nt
          )
          (ae, agg1st, if (ce.foldable) nullify(ret) else ret)
        }
        (projections, operators)
    }.unzip
    val clusterAggOperatorMap = _clusterAggOperatorMap.flatten
    // Setup expand for the 'regular' aggregate expressions.
    // only expand unfoldable children
    val regularAggExprs = aggExpressions
      .filter(e => !e.aggregateFunction.isInstanceOf[ClusterOLS] && e.children.exists(!_.foldable))
    val regularAggFunChildren = regularAggExprs
      .flatMap(_.aggregateFunction.children.filter(!_.foldable))
    val regularAggFilterAttrs = regularAggExprs.flatMap(_.filterAttributes)
    val regularAggChildren = (regularAggFunChildren ++ regularAggFilterAttrs).distinct
    val regularAggChildrenAttrMap = regularAggChildren.map { e =>
      e -> AttributeReference(e.sql, e.dataType, nullable = true)()
    }
    val regularAggChildrenAttrLookup = regularAggChildrenAttrMap.toMap
    // Setup aggregates for 'regular' aggregate expressions.
    val regularGroupId = Literal(0)
    val regularAggOperatorMap = regularAggExprs.map { ae =>
      // Perform the actual aggregation in the initial aggregate.
      val naf = ae.aggregateFunction.mapChildren { e =>
        regularAggChildrenAttrLookup.getOrElse(e, e)
      }.asInstanceOf[AggregateFunction]
      // We changed the attributes in the [[Expand]] output using expressionAttributePair.
      // So we need to replace the attributes in FILTER expression with new ones.
      val filterOpt = ae.filter.map(_.transform {
        case a: Attribute => regularAggChildrenAttrLookup.getOrElse(a, a)
      })
      val operator = Alias(ae.copy(aggregateFunction = naf, filter = filterOpt), ae.sql)()

      // Select the result of the first aggregate in the last aggregate.
      val result = First(operator.toAttribute, ignoreNulls = true)
        .toAggregateExpression(isDistinct = false, filter = Some(EqualTo(gid, regularGroupId)))

      // Some aggregate functions (COUNT) have the special property that they can return a
      // non-null result without any input. We need to make sure we return a result in this case.
      val resultWithDefault = naf.defaultResult match {
        case Some(lit) => Coalesce(Seq(result, lit))
        case None => result
      }

      // Return a Tuple3 containing:
      // i. The original aggregate expression (used for look ups).
      // ii. The actual aggregation operator (used in the first aggregate).
      // iii. The operator that selects and returns the result (used in the second aggregate).
      (ae, operator, resultWithDefault)
    }

    // Construct the regular aggregate input projection only if we need one.
    val regularAggProjection = if (regularAggExprs.nonEmpty) {
      Seq(a.groupingExpressions ++
        clusterChildren.map(nullify) ++
        Seq(regularGroupId) ++
        regularAggChildren)
    } else {
      Seq.empty[Seq[Expression]]
    }

    // Construct the distinct aggregate input projections.
    val regularAggNulls = regularAggChildren.map(nullify)
    val clusterAggProjections = clusterProjections.map { projection =>
      a.groupingExpressions ++
        projection ++
        regularAggNulls
    }

    // Construct the expand operator.
    val expand = Expand(
      regularAggProjection ++ clusterAggProjections,
      groupByAttrs ++ clusterChildrenAttrMap.map(_._2) ++
        Seq(gid) ++ regularAggChildrenAttrMap.map(_._2),
      a.child
    )

    // Construct the first aggregate operator. This de-duplicates all the children of
    // distinct operators, and applies the regular aggregate operators.
    val firstAggregateGroupBy = groupByAttrs ++ clusterGroups.keys.map(clusterChildrenAttrLookup).toSeq :+ gid
    val firstAggregate = Aggregate(
      firstAggregateGroupBy,
      firstAggregateGroupBy ++ clusterAggOperatorMap.map(_._2) ++ regularAggOperatorMap.map(_._2),
      expand
    )
    // Construct the second aggregate
    val transformations: Map[Expression, Expression] =
      (clusterAggOperatorMap.map(e => (e._1, e._3)) ++
        regularAggOperatorMap.map(e => (e._1, e._3))).toMap

    val patchedAggExpressions = a.aggregateExpressions.map { e =>
      e.transformDown {
        case e: Expression =>
          // The same GROUP BY clauses can have different forms (different names for instance) in
          // the groupBy and aggregate expressions of an aggregate. This makes a map lookup
          // tricky. So we do a linear search for a semantically equal group by expression.
          groupByMap
            .find(ge => e.semanticEquals(ge._1))
            .map(_._2)
            .getOrElse(transformations.getOrElse(e, e))
      }.asInstanceOf[NamedExpression]
    }
    val agg2nd = Aggregate(groupByAttrs, patchedAggExpressions, firstAggregate)
    agg2nd
  }

  private def nullify(e: Expression) = Literal.create(null, e.dataType)

  case class RewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
      _.containsPattern(AGGREGATE)) {
      case a: Aggregate if a.aggregateExpressions.exists {
        _.find(_.isInstanceOf[ClusterOLS]).nonEmpty
      } =>
        rewrite(a)
    }
  }
}
