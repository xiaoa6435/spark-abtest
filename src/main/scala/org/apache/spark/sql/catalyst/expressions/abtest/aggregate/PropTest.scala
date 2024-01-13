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

import java.util.Locale

import scala.util.Try

import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.abtest._
import org.apache.spark.sql.catalyst.expressions.abtest.Factor.safeIsNaN
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class PropTest(
  override val y: Expression,
  treat: Seq[Expression],
  override val confLevel: Expression,
  override val alternative: Expression,
  override val removeNaN: Expression,
  correct: Expression
) extends HypothesisTest {

  def this(children: Seq[Expression]) =
    this(
      children(0),
      Factor.getTreat(children(1), onehot = true),
      Try(children(2)).getOrElse(Literal(0.95)),
      Try(children(3)).getOrElse(Literal("two-sided")),
      Try(children(4)).getOrElse(Literal(true)),
      Try(children(5)).getOrElse(Literal(true)),
    )

  private lazy val nt: Int = treat.length

  override def inputTypes: Seq[AbstractDataType] =
    (TypeCollection(DoubleType, BooleanType) +: Seq.fill(nt)(DoubleType)) ++
      Seq(DoubleType, StringType, BooleanType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }
    checkParaTypes()
  }

  override def dataType: DataType = evaluateExpression.dataType

  override val suff: Seq[AttributeReference] = {
    Seq.range(0, nt).flatMap { i =>
      Seq(
        AttributeReference(s"n$i", DoubleType, nullable = false)(),
        AttributeReference(s"sy$i", DoubleType, nullable = false)()
      )
    }
  }

  override lazy val updateExpressions: Seq[Expression] = {
    val hasNull = y.isNull || treat.head.isNull
    val hasNaN = safeIsNaN(y) || safeIsNaN(treat.head)
    val rowRemoved = if (removeNaN == Literal(true)) hasNull || hasNaN else hasNull
    val tu = treat.flatMap { ti => Seq(ti, ti * Cast(y, DoubleType)) }
    suff.zip(tu).map { case (ei, ui) => If(!rowRemoved, ei + ui, ei) }
  }

  override lazy val evaluateExpression: Expression = {
    val ret = PropTest.PropTestFromSuff(CreateArray(suff), confLevel, alternative, correct)
    If(IsNaN(suff(0)) || IsNaN(suff(1)), Literal(null, ret.dataType), ret)
  }

  override def children: Seq[Expression] =
    (y +: treat) ++ Seq(confLevel, alternative, removeNaN, correct)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): PropTest =
    copy(y = newChildren(0), treat = newChildren.slice(1, nt + 1),
      confLevel = newChildren(nt + 1), alternative = newChildren(nt + 2),
      removeNaN = newChildren(nt + 3), correct = newChildren(nt + 4)
    )
}

object PropTest {
  val fd: FunctionDescription = (
    new FunctionIdentifier("prop_test"),
    ExpressionUtils.getExpressionInfo(classOf[PropTest], "prop_test"),
    (children: Seq[Expression]) => {
      if (children.size < 2 || children.size >= 7) {
        throw ExpressionUtils.invalidFunctionArgumentsError(
          "prop_test(y, t, confLevel = 0.95, alternative = 'two-sided'," +
            "removeNaN = true, correct = true)",
          "one of 2, 3, 4, 5, 6",
          children.length
        )
      }
      new PropTest(children)
    }
  )

  // noinspection ScalaWeakerAccess
  def propTestFromSuff(
    suff: Array[Double], confLevel: Double = 0.95, alternative: String = "two-sided",
    correct: Boolean = true, _p: Array[Double] = Array.empty
  ): (Array[Double], Double, Double, Double, Double) = {

    val k = suff.length / 2
    assert(suff.length == k * 2)
    assert(k >= 1, "k >= 1")

    val n = Array.tabulate(k) { i => suff(i * 2) }
    val x = Array.tabulate(k) { i => suff(i * 2 + 1) }
    assert(n.forall(_ >= 0.0), s"elements of ${n.toSeq} must be nonnegative")
    assert(x.forall(_ >= 0.0), s"elements of ${x.toSeq} must be nonnegative")
    assert(
      n.zip(x).forall(nx => nx._1 >= nx._2),
      s"elements of ${x.toSeq} must not be greater than those of ${n.toSeq}"
    )
    val estimate = n.zip(x).map { case (ni, xi) => xi / ni }
    val notDefinedAtP = _p == null || _p.isEmpty
    assert(notDefinedAtP || _p.length == k, s"_p${_p.toSeq} should have the same length of $k")

    val p = if (_p.nonEmpty || _p == null) {
      _p
    } else if (k == 1) {
      Array(0.5)
    } else {
      Array.fill(k)(x.sum / n.sum)
    }

    val dof = math.max(1.0, if (notDefinedAtP) k - 1 else k)
    val actual = x ++ n.zip(x).map(nx => nx._1 - nx._2)
    val expect = n.zip(p).map { case (ni, pi) => ni * pi } ++
      n.zip(p).map { case (ni, pi) => ni * (1.0 - pi) }
    val _yates = if (correct && k <= 2) 0.5 else 0.0
    val chisq = actual.zip(expect)
      .map { case (a, e) => math.pow(math.abs(a - e) - _yates, 2.0) / e }
      .sum
    val _alternative = if (k > 2 || ((k == 2) && (!notDefinedAtP))) {
      "two-sided"
    } else {
      alternative.toLowerCase(Locale.ROOT)
    }
    val pValue = Try(_alternative match {
      case "greater" if k == 2 && notDefinedAtP =>
        val delta = estimate(1) - estimate.head
        val z = math.signum(delta) * math.sqrt(chisq)
        1.0 - StatUtils.pnorm(z)
      case "less" if k == 2 && notDefinedAtP =>
        val delta = estimate(1) - estimate.head
        val z = math.signum(delta) * math.sqrt(chisq)
        StatUtils.pnorm(z)
      case "greater" if k == 1 =>
        val z = math.signum(estimate.head - p.head) * math.sqrt(chisq)
        1.0 - StatUtils.pnorm(z)
      case "less" if k == 1 =>
        val z = math.signum(estimate.head - p.head) * math.sqrt(chisq)
        StatUtils.pnorm(z)
      case _ =>
        1.0 - StatUtils.pchisq(math.abs(chisq), dof)
    }).getOrElse(Double.NaN)

    val (lower, upper) = k match {
      case 2 if notDefinedAtP =>
        val delta = estimate(1) - estimate.head
        val gm = 1.0 / n.map(1.0 / _).sum
        val yates = math.min(_yates, math.abs(delta) * gm)
        val stderr = math.sqrt(estimate.zip(n).map { case (pi, ni) => pi * (1.0 - pi) / ni }.sum)
        _alternative match {
          case "two-sided" =>
            val s = StatUtils.qnorm((1.0 + confLevel) / 2.0)
            val width = s * stderr + yates / gm
            (math.max(delta - width, -1), math.min(delta + width, 1.0))
          case "greater" =>
            val s = StatUtils.qnorm(confLevel)
            val width = s * stderr + yates / gm
            (math.max(delta - width, -1.0), 1.0)
          case _ =>
            val s = StatUtils.qnorm(confLevel)
            val width = s * stderr + yates / gm
            (-1.0, math.min(delta + width, 1.0))
        }
      case 1 =>
        val s = if (_alternative == "two-sided") (1.0 + confLevel) / 2.0 else confLevel
        val z = StatUtils.qnorm(s)
        val (n0, x0, p0) = (n.head, x.head, p.head)
        val yates = math.min(_yates, math.abs(x0 - n0 * p0))
        val z22n = math.pow(z, 2.0) / (2.0 * n0)
        val lo = estimate.head - yates / n0
        val _lower = lo + z22n - z * math.sqrt(lo * (1.0 - lo) / n0 + z22n / (2.0 * n0))
        val lower = _lower / (1.0 + 2.0 * z22n)
        val up = estimate.head + yates / n0
        val _upper = up + z22n + z * math.sqrt(up * (1.0 - up) / n0 + z22n / (2.0 * n0))
        val upper = _upper / (1.0 + 2.0 * z22n)
        _alternative match {
          case "two-sided" =>
            (math.max(lower, 0.0), math.min(upper, 1.0))
          case "greater" =>
            (math.max(lower, 0.0), 1.0)
          case _ =>
            (0.0, math.min(upper, 1.0))
        }
      case _ =>
        (Double.NaN, Double.NaN)
    }
    (estimate, pValue, lower, upper, chisq)
  }

  private case class PropTestFromSuff(
    suff: Expression, confLevel: Expression, alternative: Expression,
    correct: Expression, p: Expression = CreateArray(Seq())
  ) extends org.apache.spark.sql.catalyst.expressions.abtest.QuinaryExpression {
    override def nullable: Boolean = true

    override def dataType: DataType = StructType(Seq(
      StructField("estimate", ArrayType(DoubleType, containsNull = false), nullable = false),
      StructField("pvalue", DoubleType, nullable = false),
      StructField("lower", DoubleType, nullable = false),
      StructField("upper", DoubleType, nullable = false),
      StructField("chisq", DoubleType, nullable = false)
    ))

    override def prettyName: String = "prop_test_from_suff"

    override def nullSafeEval(
      input1: Any, input2: Any, input3: Any, input4: Any, input5: Any
    ): Any = {
      val ret = StatUtils.toObjectArr(PropTest.propTestFromSuff(
        input1.asInstanceOf[ArrayData].toDoubleArray,
        input2.asInstanceOf[Double],
        input3.asInstanceOf[UTF8String].toString,
        input4.asInstanceOf[Boolean],
        input5.asInstanceOf[ArrayData].toDoubleArray
      ))
      InternalRow(ret: _*)
    }

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val rowClass = classOf[GenericInternalRow].getName
      val statUtilsClass = StatUtils.getClass.getCanonicalName.stripSuffix("$")
      val propTestClass = PropTest.getClass.getCanonicalName.stripSuffix("$")
      val ret = ctx.freshName("ret")
      val retLength = this.dataType.asInstanceOf[StructType].length
      val valCodes = nullSafeCodeGen(ctx, ev, (suff, confLevel, alternative, correct, p) => {
        s"""
           |$ret = $statUtilsClass.toObjectArr(
           |  $propTestClass.propTestFromSuff(
           |    $suff.toDoubleArray(), $confLevel, $alternative.toString(),
           |    $correct, $p.toDoubleArray()
           |  )
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

    override def children: Seq[Expression] = Seq(suff, confLevel, alternative, correct, p)

    override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
      copy(suff = newChildren(0), confLevel = newChildren(1), alternative = newChildren(2),
        correct = newChildren(3), p = newChildren(4))
  }
}
