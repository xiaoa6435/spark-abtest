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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.abtest.{Confint, StatUtils}
import org.apache.spark.sql.catalyst.expressions.abtest.aggregate.OLS._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class OLSFromSuff(suff: Expression, stdErrorType: Expression, p: Int, method: String)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(ArrayType(DoubleType, containsNull = false), StringType)

  override def dataType: DataType = StructType(Seq(
    StructField("b", ArrayType(DoubleType, containsNull = false), nullable = false),
    StructField("stderr", ArrayType(DoubleType, containsNull = false), nullable = false),
    StructField("dof", DoubleType, nullable = false),
    StructField("ym", ArrayType(DoubleType, containsNull = false), nullable = false),
    StructField("xm", ArrayType(DoubleType, containsNull = false), nullable = false)
  ))

  override def nullable: Boolean = true

  override def prettyName: String = "ols_from_suff"

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val suff = input1.asInstanceOf[ArrayData].toDoubleArray
    val stdErrorType = input2.asInstanceOf[UTF8String].toString
    val (b, stderr, dof, ym, xm) = OLSFromSuff.olsFromSuff(suff, p, stdErrorType, method)
    InternalRow(
      new GenericArrayData(b),
      new GenericArrayData(stderr),
      dof,
      new GenericArrayData(ym),
      new GenericArrayData(xm)
    )
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val rowClass = classOf[GenericInternalRow].getName
    val statUtilsClass = StatUtils.getClass.getCanonicalName.stripSuffix("$")
    val olsFromSuffClass = OLSFromSuff.getClass.getCanonicalName.stripSuffix("$")
    val ret = ctx.freshName("ret")
    val retLength = this.dataType.asInstanceOf[StructType].length
    val valCodes = nullSafeCodeGen(ctx, ev, (suff, stderr) => {
      s"""
         |$ret = $statUtilsClass.toObjectArr(
         |  $olsFromSuffClass.olsFromSuff(
         |    $suff.toDoubleArray(), $p, $stderr.toString(), "$method")
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

  override def left: Expression = suff

  override def right: Expression = stdErrorType

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression
  ): OLSFromSuff =
    copy(suff = newLeft, stdErrorType = newRight)
}

object OLSFromSuff {
  def packOLSRet(
    ols: Expression, p: Int, confLevel: Expression,
    method: String, isFactor: Boolean
  ): Expression = {
    val b = Seq.range(0, p).map(i => GetArrayItem(GetStructField(ols, 0), Literal(i)))
    val stderr = Seq.range(0, p).map(i => GetArrayItem(GetStructField(ols, 1), Literal(i)))
    val dof = GetStructField(ols, 2)
    val ym = Seq.range(0, p).map(i => GetArrayItem(GetStructField(ols, 3), Literal(i)))
    val xm = Seq.range(0, p).map(i => GetArrayItem(GetStructField(ols, 4), Literal(i)))
    val retArr = b.zip(stderr).zip(ym).zip(xm).map { case (((delta, stderr), ym), xm) =>
      Confint(
        delta, stderr, dof, confLevel, Literal("two-sided"),
        CreateArray(Seq(ym, xm, Literal(Double.NaN), Literal(Double.NaN)))
      )
    }

    val ret = method match {
      case "ols" | "cluster_ols" => CreateArray(retArr)
      case _ if !isFactor =>
        val myx = CreateArray(Seq(ym(0), ym(1), xm(0), xm(1)))
        Confint(b(1), stderr(1), dof, confLevel, Literal("two-sided"), myx)
      case "ttest" | "cluster_ttest" | "anova" | "cluster_anova" => CreateArray(retArr.slice(0, p))
      case "ancova1" | "cluster_ancova1" => CreateArray(retArr.slice(0, p - 1))
      case "ancova2" | "cluster_ancova2" => CreateArray(retArr.slice(0, (p + 1) / 2))
    }
    val nullRet = Literal(null, ret.dataType)
    If(dof <= Literal(0.0), nullRet, ret)
  }

  // noinspection ScalaWeakerAccess
  def olsFromSuff(
    suff: Array[Double], p: Int,
    stdErrorType: String, method: String
  ): (Array[Double], Array[Double], Double, Array[Double], Array[Double]) = {
    val constSuffSize = suffSize(p, "const")
    val robustSuffSize = suffSize(p, "hc0")
    val clusterSuffSize = robustSuffSize + 1
    assert(Seq(constSuffSize, robustSuffSize, clusterSuffSize).contains(suff.length))

    def subSlice(v: Array[Double], offset: Int, len: Int) = v.slice(offset, offset + len)

    val _yxtyx = suff.slice(0, constSuffSize - 1)
    val isAllZero = _yxtyx.forall(_ == 0.0)
    val existsNaN = _yxtyx.exists(_.isNaN)
    val isAncova2 = method.contains("ancova2")
    val (yxtyx, xm) = if (isAncova2) {
      val xm = _yxtyx(OLS.ijToInd(1, p / 2 + 1, p + 1)) / _yxtyx(OLS.ijToInd(1, 1, p + 1))
      (deXMean(_yxtyx, xm), xm)
    } else {
      (_yxtyx, Double.NaN)
    }
    val xtxSize = p * (p + 1) / 2
    val n = subSlice(suff, 1 + p + xtxSize, 1).head

    def olsRes(yxtyx: Array[Double]) = {
      val yty = yxtyx.head
      val xty = subSlice(yxtyx, 1, p)
      val xtx = subSlice(yxtyx, 1 + p, xtxSize)
      Try(ols(xtx, xty, yty, n)).getOrElse(
        Array.fill(p)(Double.NaN),
        Array.fill(p)(Double.NaN),
        Array.fill(xtxSize)(Double.NaN)
      )
    }

    val (b, stderr, xtxInv) = olsRes(yxtyx)
    val dof = if (isAllZero || n - p <= 0 || existsNaN) {
      0.0
    } else {
      n - p
    }
    val _stdErrorType = stdErrorType.toLowerCase(Locale.ROOT)
    val (iym, ixm) = getMean(suff, p, method)
    if (_stdErrorType == "const") {
      return (b, stderr, dof, iym, ixm)
    }
    val rb = if (isAncova2) olsRes(_yxtyx)._1 else b
    val ib = 1.0 +: rb.map(e => -e)
    val ibib = lowerCross(p + 1, (ik, jk) => if (ik == jk) ib(ik) * ib(jk) else ib(ik) * ib(jk))
    val ibibSize = ibib.length
    val xexe = subSlice(suff, 1 + p + xtxSize + 1, xtxSize * ibibSize)
    val ncl = subSlice(suff, 1 + p + xtxSize + 1 + xtxSize * ibibSize, 1).headOption.getOrElse(-1.0)
    val w = if (ncl >= 0.0 && _stdErrorType == "hc1") {
      (n - 1.0) / dof * ncl / (ncl - 1.0)
    } else if (_stdErrorType == "hc1") {
      n / dof
    } else {
      1.0
    }
    val _meat = lowerCross(p, (i, j) => {
      val k = ijToInd(i, j, p)
      val ixexe = xexe.slice(k * ibibSize, (k + 1) * ibibSize)
      ixexe.zip(ibib).map { case (e1, e2) => e1 * e2 }.sum * w
    }).toArray
    val meat = if (isAncova2) deXMean(_meat, xm) else _meat
    val newStderr = Array.range(0, p).map { i =>
      val xi = Array.range(0, p).map(k => xtxInv(ijToInd(i, k, p)))
      val meatxi = Array.range(0, p).map { j =>
        val mk = Array.range(0, p).map(k => meat(ijToInd(j, k, p)))
        xi.zip(mk).map { e => e._1 * e._2 }.sum
      }
      math.sqrt(xi.zip(meatxi).map { e => e._1 * e._2 }.sum)
    }
    (b, newStderr, dof, iym, ixm)
  }

  private def getMean(
    suff: Array[Double], p: Int, method: String
  ): (Array[Double], Array[Double]) = {
    def iSuff(j: Int, nt: Int) = {
      val ret = Array.range(1, nt + 1).map { i => suff(ijToInd(i, j, p + 1)) }
      (ret.head - ret.tail.sum) +: ret.tail
    }

    method match {
      case "anova" | "cluster_anova" | "ttest" | "cluster_ttest" =>
        val yi = iSuff(0, p)
        val ni = iSuff(1, p)
        val ym = yi.zip(ni).map { case (yii, nii) => yii / nii }
        (ym, Array.fill(p)(Double.NaN))
      case "ancova1" | "cluster_ancova1" | "ancova2" | "cluster_ancova2" =>
        val nt = if (method.contains("ancova1")) p - 1 else (p + 1) / 2
        val yi = iSuff(0, nt)
        val xi = iSuff(nt + 1, nt)
        val ni = iSuff(1, nt)
        val ym = yi.zip(ni).map { case (yii, nii) => yii / nii }
        val xm = xi.zip(ni).map { case (xii, nii) => xii / nii }
        (ym, xm)
      case "ols" | "cluster_ols" =>
        (Array.fill(p)(Double.NaN), Array.fill(p)(Double.NaN))
    }
  }

  private def deXMean(yxtyx: Array[Double], xm: Double): Array[Double] = {
    val p = (math.sqrt(1 + 8 * yxtyx.length) - 1).round.toInt / 2
    val nt = p / 2
    val dxm = OLS.lowerCross(p, (i, j) => {
      (i, j) match {
        case (i, j) if i < p - nt && j >= p - nt =>
          yxtyx(OLS.ijToInd(i, j - nt, p)) * xm
        case (i, j) if i >= p - nt && j >= p - nt =>
          yxtyx(OLS.ijToInd(i, j - nt, p)) * xm +
            yxtyx(OLS.ijToInd(i - nt, j, p)) * xm -
            yxtyx(OLS.ijToInd(i - nt, j - nt, p)) * xm * xm
        case _ =>
          0.0
      }
    })
    yxtyx.zip(dxm).map { case (e, dx) => e - dx }
  }
}