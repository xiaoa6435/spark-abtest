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

import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class Confint(
  deltaM: Expression, stderr: Expression,
  dof: Expression = Literal(Double.PositiveInfinity),
  confLevel: Expression = Literal(0.95d),
  alternative: Expression = Literal("two-sided"),
  myx: Expression = Literal(Array.fill(4)(Double.NaN))
) extends SeptenaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def nullable: Boolean = false

  override def nullSafeEval(
    input1: Any,
    input2: Any,
    input3: Any,
    input4: Any,
    input5: Any,
    input6: Any,
    input7: Option[Any]
  ): Any = {
    val ret = StatUtils.toObjectArr(Confint.confint(
      input1.asInstanceOf[Double],
      input2.asInstanceOf[Double],
      input3.asInstanceOf[Double],
      input4.asInstanceOf[Double],
      input5.asInstanceOf[UTF8String].toString,
      input6.asInstanceOf[ArrayData].toDoubleArray(),
    ))
    InternalRow(ret: _*)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val statUtilsClass = StatUtils.getClass.getCanonicalName.stripSuffix("$")
    val confintClass = Confint.getClass.getCanonicalName.stripSuffix("$")
    val ret = ctx.freshName("ret")
    val retLength = this.dataType.asInstanceOf[StructType].length
    val valCodes = nullSafeCodeGen(ctx, ev, (m, s, dof, cl, alt, myx, _) => {
      s"""
         |$ret = $statUtilsClass.toObjectArr(
         |  $confintClass.confint($m, $s, $dof, $cl, $alt.toString(), $myx.toDoubleArray())
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

  override def dataType: DataType = StructType(Seq(
    StructField("delta", DoubleType, nullable = false),
    StructField("stderr", DoubleType, nullable = false),
    StructField("statistic", DoubleType, nullable = false),
    StructField("pvalue", DoubleType, nullable = false),
    StructField("lower", DoubleType, nullable = false),
    StructField("upper", DoubleType, nullable = false),
    StructField("my", DoubleType, nullable = false),
    StructField("mx", DoubleType, nullable = false),
    StructField("y0", DoubleType, nullable = false),
    StructField("x0", DoubleType, nullable = false),
  ))

  override def inputTypes: Seq[AbstractDataType] =
    Seq(DoubleType, DoubleType, DoubleType, DoubleType, StringType, ArrayType(DoubleType))

  override def children: Seq[Expression] = Seq(deltaM, stderr, dof, confLevel, alternative, myx)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Confint = {
    copy(
      deltaM = newChildren(0), stderr = newChildren(1), dof = newChildren(2),
      confLevel = newChildren(3), alternative = newChildren(4), myx = newChildren(5)
    )
  }
}

object Confint {
  // noinspection ScalaStyle
  val fd: FunctionDescription = (
    new FunctionIdentifier("confint"),
    ExpressionUtils.getExpressionInfo(classOf[Confint], "confint"),
    (children: Seq[Expression]) => children.size match {
      case 2 => Confint(children.head, children(1))
      case 3 => Confint(children.head, children(1), children(2))
      case 4 => Confint(children.head, children(1), children(2), children(3))
      case 5 => Confint(children.head, children(1), children(2), children(3), children(4))
      case 6 => Confint(children.head, children(1), children(2), children(3), children(4), children(5))
      case _ => throw ExpressionUtils.invalidFunctionArgumentsError(
        "confint", "one of 2, 3, 4, 5, 6", children.length
      )
    }
  )

  def confint(
    delta: Double, stderr: Double, dof: Double = Double.PositiveInfinity,
    confLevel: Double = 0.95, alternative: String = "two-sided",
    myx: Array[Double] = Array.fill(4)(Double.NaN)
  ): Array[Any] = {

    if (delta.isNaN || stderr.isNaN || dof.isNaN || confLevel.isNaN ||
      stderr <= 0.0 || dof <= 0.0 || confLevel <= 0.0 || confLevel >= 1.0 ||
      !Seq("two-sided", "less", "greater").contains(alternative)) {
      return Array(delta, stderr) ++ Array.fill(8)(Double.NaN)
    }

    val t = delta / stderr
    val (pValue, lower, upper) = alternative.toLowerCase(Locale.ROOT) match {
      case "two-sided" =>
        val pValue = 2.0 - StatUtils.pt(math.abs(t), dof) * 2.0
        val s = StatUtils.qt(0.5 + confLevel / 2, dof)
        (pValue, delta - s * stderr, delta + s * stderr)
      case "greater" =>
        val pValue = 1.0 - StatUtils.pt(t, dof)
        val s = StatUtils.qt(confLevel, dof)
        (pValue, delta - s * stderr, Double.PositiveInfinity)
      case "less" =>
        val pValue = StatUtils.pt(t, dof)
        val s = StatUtils.qt(confLevel, dof)
        (pValue, Double.NegativeInfinity, delta + s * stderr)
    }
    Array(delta, stderr, t, pValue, lower, upper) ++ myx
  }
}
