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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.abtest.{Confint, Factor, StatUtils}
import org.apache.spark.sql.catalyst.expressions.abtest.Factor.safeIsNaN
import org.apache.spark.sql.catalyst.expressions.abtest.aggregate.TTest.TTestFromSuff
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class TTest(
  override val y: Expression,
  t: Expression,
  override val confLevel: Expression = Literal(0.95),
  override val alternative: Expression = Literal("two-sided"),
  override val varEqual: Expression = Literal(false),
  override val removeNaN: Expression = Literal(true),
  x: Expression = Literal(0.0),
  isTTest: Boolean = true
) extends HypothesisTest {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      DoubleType, TypeCollection(DoubleType, BooleanType),
      DoubleType, StringType, BooleanType, BooleanType, DoubleType
    )

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }
    checkParaTypes()
  }

  override val suff: Seq[AttributeReference] = {
    Seq.range(0, 2).flatMap { i =>
      Seq(
        AttributeReference(s"n$i", DoubleType, nullable = false)(),
        AttributeReference(s"sy$i", DoubleType, nullable = false)(),
        AttributeReference(s"sy2$i", DoubleType, nullable = false)(),
        AttributeReference(s"sx$i", DoubleType, nullable = false)(),
        AttributeReference(s"sx2$i", DoubleType, nullable = false)(),
        AttributeReference(s"sxy$i", DoubleType, nullable = false)()
      ).slice(0, if (isTTest) 3 else 6)
    }
  }

  override lazy val updateExpressions: Seq[Expression] = {
    val hasNull = if (isTTest) {
      y.isNull || t.isNull
    } else {
      y.isNull || x.isNull || t.isNull
    }
    val hasNaN = if (isTTest) {
      safeIsNaN(y) || safeIsNaN(t)
    } else {
      safeIsNaN(y) || safeIsNaN(x) || safeIsNaN(t)
    }
    val rowRemoved = if (removeNaN == Literal(true)) hasNull || hasNaN else hasNull
    val tu = Factor.getTreat(t, onehot = true).flatMap { ti =>
      Seq(one, y, y * y, x, x * x, x * y)
        .slice(0, if (isTTest) 3 else 6)
        .map(u => Cast(ti, DoubleType) * u)
    }
    suff.zip(tu).map { case (ei, ui) => If(!rowRemoved, ei + ui, ei) }
  }

  override lazy val evaluateExpression: Expression = {
    val ret = TTestFromSuff(CreateArray(suff), isTTest, confLevel, alternative, varEqual)
    val n0 = suff.find(_.name == "n0").getOrElse(zero)
    val n1 = suff.find(_.name == "n1").getOrElse(zero)
    If(
      n1 <= one || n0 <= one || IsNaN(suff(0)) || IsNaN(suff(1)) || IsNaN(suff(4)),
      Literal(null, ret.dataType),
      ret
    )
  }

  override def children: Seq[Expression] = Seq(y, t, confLevel, alternative, varEqual, removeNaN, x)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): TTest =
    copy(
      y = newChildren(0), t = newChildren(1),
      confLevel = newChildren(2), alternative = newChildren(3),
      varEqual = newChildren(4), removeNaN = newChildren(5), x = newChildren(6)
    )
}

object TTest {
  val fd: FunctionDescription = (
    new FunctionIdentifier("ttest"),
    ExpressionUtils.getExpressionInfo(this.getClass, "ttest"),
    (children: Seq[Expression]) => {
      if (children.size < 2 || children.size >= 7) {
        throw ExpressionUtils.invalidFunctionArgumentsError(
          "ttest(y, t, confLevel = 0.95, alternative = 'two-sided'," +
            "varEqual = false, removeNaN = true)",
          "one of 3, 4, 5, 6, 7", children.length
        )
      }
      val y = children(0)
      val t = children(1)
      val confLevel = children.applyOrElse(2, (_: Int) => Literal(0.95))
      val alternative = children.applyOrElse(3, (_: Int) => Literal("two-sided"))
      val varEqual = children.applyOrElse(4, (_: Int) => Literal(false))
      val removeNaN = children.applyOrElse(5, (_: Int) => Literal(true))
      TTest(y, t, confLevel, alternative, varEqual, removeNaN)
    }
  )

  val cupedFd: FunctionDescription = (
    new FunctionIdentifier("cuped"),
    ExpressionUtils.getExpressionInfo(this.getClass, "cuped"),
    (children: Seq[Expression]) => {
      if (children.size < 3 || children.size >= 8) {
        throw ExpressionUtils.invalidFunctionArgumentsError(
          "cuped(y, t, x, confLevel = 0.95, alternative = 'two-sided'," +
            "varEqual = false, removeNaN = true)",
          "one of 3, 4, 5, 6, 7", children.length
        )
      }
      val y = children(0)
      val t = children(1)
      val x = children(2)
      val confLevel = children.applyOrElse(3, (_: Int) => Literal(0.95))
      val alternative = children.applyOrElse(4, (_: Int) => Literal("two-sided"))
      val varEqual = children.applyOrElse(5, (_: Int) => Literal(false))
      val removeNaN = children.applyOrElse(6, (_: Int) => Literal(true))
      TTest(y, t, confLevel, alternative, varEqual, removeNaN, x, isTTest = false)
    }
  )

  // noinspection ScalaWeakerAccess
  def ttestFromSuff(
    suff: Array[Double],
    confLevel: Double = 0.95,
    alternative: String = "two-sided",
    varEqual: Boolean = false
  ): (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) = {
    assert(Array(6, 12).contains(suff.length))

    val isAllZero = suff.forall(_ == 0.0)
    val existsNaN = suff.exists(_.isNaN)
    if (isAllZero || existsNaN) {
      return (Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
        Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN)
    }
    val (delta, v0, v1, n0, n1, y0, y1, x0, x1, theta) = if (suff.length == 6) {
      val Array(n0, sy0, sy20, n1, sy1, sy21) = suff
      val (v0, v1) = (sy20 - sy0 * sy0 / n0, sy21 - sy1 * sy1 / n1)
      val (y0, y1) = (sy0 / n0, sy1 / n1)
      val delta = y1 - y0
      (delta, v0, v1, n0, n1, y0, y1, Double.NaN, Double.NaN, Double.NaN)
    } else {
      val Array(n0, sy0, sy20, sx0, sx20, sxy0, n1, sy1, sy21, sx1, sx21, sxy1) = suff
      val n = n0 + n1
      val sx = sx0 + sx1
      val sy = sy0 + sy1
      val sx2 = sx20 + sx21
      val sxy = sxy0 + sxy1
      val covxy = sxy - sx * sy / n
      val varx = sx2 - sx * sx / n
      val theta = covxy / varx
      val (y0, y1, x0, x1) = (sy0 / n0, sy1 / n1, sx0 / n0, sx1 / n1)
      val delta = y1 - y0 - theta * (x1 - x0)
      val _v0 = sy20 - 2 * theta * sxy0 + theta * theta * sx20
      val _v1 = sy21 - 2 * theta * sxy1 + theta * theta * sx21
      val v0 = _v0 - math.pow(sy0 - theta * sx0, 2.0) / n0
      val v1 = _v1 - math.pow(sy1 - theta * sx1, 2.0) / n1
      (delta, v0, v1, n0, n1, y0, y1, x0, x1, theta)
    }

    val (stderr, dof) = if (varEqual) {
      val stderr = math.sqrt((v0 + v1) / (n0 + n1 - 2) * (1.0 / n0 + 1.0 / n1))
      val dof = n0 + n1 - 2
      (stderr, dof)
    } else {
      val stderr = math.sqrt(v0 / (n0 - 1) / n0 + v1 / (n1 - 1) / n1)
      val dof = math.pow(stderr, 4) / (
        math.pow(v0 / n0, 2) / math.pow(n0 - 1, 3) +
          math.pow(v1 / n1, 2) / math.pow(n1 - 1, 3)
        )
      (stderr, dof)
    }
    val ret = Confint.confint(delta, stderr, dof, confLevel, alternative)
    val Array(_, _, tValue, pValue, lower, upper, _, _, _, _) = ret.map(_.asInstanceOf[Double])
    (delta, stderr, tValue, pValue, lower, upper, y0, y1, x0, x1, theta)
  }

  case class TTestFromSuff(
    suff: Expression,
    isTTest: Boolean,
    confLevel: Expression,
    alternative: Expression,
    varEqual: Expression,
  ) extends QuaternaryExpression {

    override def nullable: Boolean = true

    override def dataType: DataType = {
      val dt = Seq(
        StructField("delta", DoubleType, nullable = false),
        StructField("stderr", DoubleType, nullable = false),
        StructField("tvalue", DoubleType, nullable = false),
        StructField("pvalue", DoubleType, nullable = false),
        StructField("lower", DoubleType, nullable = false),
        StructField("upper", DoubleType, nullable = false),
        StructField("y0", DoubleType, nullable = false),
        StructField("y1", DoubleType, nullable = false),
        StructField("x0", DoubleType, nullable = false),
        StructField("x1", DoubleType, nullable = false),
        StructField("theta", DoubleType, nullable = false)
      )
      if (isTTest) StructType(dt.init.init.init) else StructType(dt)
    }

    override def prettyName: String = "ttest_from_suff"

    override def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any = {
      val ret = StatUtils.toObjectArr(TTest.ttestFromSuff(
        input1.asInstanceOf[ArrayData].toDoubleArray,
        input2.asInstanceOf[Double],
        input3.asInstanceOf[UTF8String].toString,
        input4.asInstanceOf[Boolean]
      ))
      InternalRow(ret: _*)
    }

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val rowClass = classOf[GenericInternalRow].getName
      val statUtilsClass = StatUtils.getClass.getCanonicalName.stripSuffix("$")
      val ttestClass = TTest.getClass.getCanonicalName.stripSuffix("$")
      val ret = ctx.freshName("ret")
      val retLength = this.dataType.asInstanceOf[StructType].length
      val valCodes = nullSafeCodeGen(ctx, ev, (suff, confLevel, alternative, varEqual) => {
        s"""
           |$ret = $statUtilsClass.toObjectArr(
           |  $ttestClass.ttestFromSuff(
           |    $suff.toDoubleArray(), $confLevel, $alternative.toString(), $varEqual)
           |);
           |""".stripMargin
      })
      ev.copy(code =
        code"""
              |Object[] $ret = new Object[$retLength];
              |${valCodes.code}
              |${ev.value} = new $rowClass($ret);
              |$ret = null;
            """.stripMargin, isNull = FalseLiteral)
    }

    override def first: Expression = suff

    override def second: Expression = alternative

    override def third: Expression = confLevel

    override def fourth: Expression = varEqual

    override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression, newFourth: Expression
    ): Expression =
      copy(suff = newFirst, confLevel = newSecond, alternative = newThird, varEqual = newFourth)
  }
}
