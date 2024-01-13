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

package org.apache.spark.sql.catalyst.expressions.abtest

import java.util.Locale

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types._

case class Factor(x: Expression, levels: Expression) extends Unevaluable {

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(DoubleType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }
    if (!levels.foldable) {
      return TypeCheckFailure(s"The levels must be a constant array literal, but got $levels")
    }
    val p = levels match {
      case Literal(arr, ArrayType(_, _)) => arr.asInstanceOf[ArrayData].numElements()
      case e: CreateArray => e.children.length
      case _ => 0
    }
    if (p <= 0) {
      return TypeCheckFailure(s"The levels must be a constant array literal, but got $levels")
    }
    TypeCheckSuccess
  }

  override def children: Seq[Expression] = Seq(x, levels)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Factor =
    copy(x = newChildren.head, levels = newChildren(1))
}

object Factor {
  val fd: FunctionDescription = (
    new FunctionIdentifier("factor"),
    ExpressionUtils.getExpressionInfo(this.getClass, "factor"),
    (children: Seq[Expression]) => children.size match {
      case 2 => new Factor(children.head, children.last)
      case _ => throw ExpressionUtils.invalidFunctionArgumentsError(
        "factor", "2", children.length
      )
    }
  )

  def getTreat(t: Expression, onehot: Boolean): Seq[Expression] = {
    val zero = Literal(0.0)
    val one = Literal(1.0)
    val litTrue = Literal(true)
    val treat = t match {
      case e if e == null => Seq(litTrue)
      case e if e.dataType == BooleanType => Seq(!e, e)
      case Factor(x, levels) =>
        val p = levels match {
          case Literal(arr, ArrayType(_, _)) => arr.asInstanceOf[ArrayData].numElements()
          case e: CreateArray => e.children.length
        }
        Seq.tabulate(p) { i => GetArrayItem(levels, Literal(i)) === x }
      case e if e.foldable => Seq(litTrue)
      case e if e.dataType.isInstanceOf[NumericType] =>
        val t2dbl = Cast(t, DoubleType)
        Seq(t2dbl <= zero, t2dbl > zero)
    }

    val _intercept = if (onehot) If(treat.head, one, zero) else one
    val intercept = t.dataType match {
      case DoubleType | FloatType =>
        If(
          IsNull(t),
          Literal(null, DoubleType),
          If(IsNaN(t), Literal(Double.NaN, DoubleType), _intercept)
        )
      case _ =>
        If(IsNull(t), Literal(null, DoubleType), _intercept)
    }
    intercept +: treat.tail.map(ti => If(ti, one, zero))
  }

  def isFactor(children: Seq[Expression], method: String): Boolean = {
    method.toLowerCase(Locale.ROOT) match {
      case "ols" | "cluster_ols" => false
      case _ =>
        children(1) match {
          case Factor(_, _: CreateArray) => true
          case Factor(_, Literal(_, ArrayType(_, _))) => true
          case _ => false
        }
    }
  }

  def safeIsNaN(e: Expression): Expression = {
    e.dataType match {
      case DoubleType | FloatType => IsNaN(e)
      case _ => Literal(false)
    }
  }
}
