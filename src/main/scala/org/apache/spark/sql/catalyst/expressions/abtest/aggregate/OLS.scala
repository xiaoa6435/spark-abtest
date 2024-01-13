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

import org.apache.commons.math3.linear.{Array2DRowRealMatrix, RealMatrix, RRQRDecomposition}

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.abtest.Factor
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types._

case class OLS(
  override val y: Expression,
  X: Seq[Expression],
  override val confLevel: Expression,
  override val stdErrorType: Expression,
  override val removeNaN: Expression,
  method: String, isFactor: Boolean
) extends HypothesisTest {

  def this(children: Seq[Expression], method: String) = {
    this(
      children.head,
      OLS.toX(children, method),
      children.applyOrElse(OLS.offset(method), (_: Int) => Literal(0.95)),
      children.applyOrElse(OLS.offset(method) + 1, (_: Int) => Literal("const")),
      children.applyOrElse(OLS.offset(method) + 2, (_: Int) => Literal(true)),
      method,
      Factor.isFactor(children, method)
    )
  }

  private lazy val p: Int = X.length

  override val suff: Seq[AttributeReference] = {
    val suffBase = OLS.lowerCross(p + 1, (i, j) => {
      val attrName = (i, j) match {
        case (0, 0) => "yty"
        case (0, _) => s"xty_$j"
        case _ => s"xtx_${i}_$j"
      }
      AttributeReference(attrName, DoubleType, nullable = false)()
    })
    val n = AttributeReference("n", DoubleType, nullable = false)()
    val suffxe = if (stdErrorType == Literal("const")) {
      Nil
    } else {
      OLS.lowerCross(p, (i, j) => {
        OLS.lowerCross(p + 1, (ii, jj) => {
          val attrName = s"xe_${i}_${j}_${ii}_$jj"
          AttributeReference(attrName, DoubleType, nullable = false)()
        })
      }).flatten
    }
    (suffBase :+ n) ++ suffxe
  }

  override def inputTypes: Seq[AbstractDataType] =
    (DoubleType +: Seq.fill(p)(DoubleType)) ++ Seq(DoubleType, StringType, BooleanType)

  override def dataType: DataType = OLS.dataTypes(method, isFactor)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    } else if (p < 1) {
      return TypeCheckFailure(s"The x must not empty, but got ${X.map(_.sql)}")
    }
    checkParaTypes()
  }

  override def prettyName: String = method

  override lazy val updateExpressions: Seq[Expression] = {
    val yx = y +: X
    val suffUpdate = OLS.getSuff(yx, _stdErrorType, removeNaN)
    suffUpdate.zip(suff).map { case (e1, e2) => e1 + e2 }
  }

  override lazy val evaluateExpression: Expression = {
    val ols = new OLSFromSuff(CreateArray(suff), stdErrorType, p, method)
    OLSFromSuff.packOLSRet(ols, p, confLevel, method, isFactor)
  }

  override def stringArgs: Iterator[Any] =
    super.stringArgs.filter(_.isInstanceOf[Expression])

  override def children: Seq[Expression] =
    (y +: X) ++ Seq(confLevel, stdErrorType, removeNaN)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(y = newChildren(0), X = newChildren.slice(1, p + 1), confLevel = newChildren(p + 1),
      stdErrorType = newChildren(p + 2), removeNaN = newChildren(p + 3))
}

object OLS {
  def fd(method: String): FunctionDescription = (
    new FunctionIdentifier(method),
    ExpressionUtils.getExpressionInfo(classOf[OLS], method),
    (children: Seq[Expression]) => {
      val isCluster = method.startsWith("cluster")
      children.size match {
        case 3 | 4 | 5 | 6 if OLS.offset(method) == 2 && isCluster =>
          new ClusterOLS(children, method)
        case 4 | 5 | 6 | 7 if OLS.offset(method) == 3 && isCluster =>
          new ClusterOLS(children, method)
        case 2 | 3 | 4 | 5 if OLS.offset(method) == 2 => new OLS(children, method)
        case 3 | 4 | 5 | 6 if OLS.offset(method) == 3 => new OLS(children, method)
        case _ =>
          val defaultStderr = if (isCluster) "HC1" else "const"
          val methodDesc = (method match {
            case "cluster_ols" => s"$method(y, x, c"
            case "ols" => s"$method(y, x"
            case _ if isCluster => s"$method(y, t, x, c"
            case _ => s"$method(y, t, x"
          }) + s"confLevel = 0.95, stdErrorType = '$defaultStderr', removeNaN = true)"
          throw ExpressionUtils.invalidFunctionArgumentsError(
            methodDesc, "one of 2, 3, 4, 5, 6", children.length
          )
      }
    }
  )

  val olsFD: FunctionDescription = fd("ols")
  val anovaFD: FunctionDescription = fd("anova")
  val ancova1FD: FunctionDescription = fd("ancova1")
  val ancova2FD: FunctionDescription = fd("ancova2")

  private def mulSimplify(e1: Expression, e2: Expression): Expression = {
    (e1, e2) match {
      case (_, Literal(1.0, DoubleType)) => e1
      case (Literal(1.0, DoubleType), _) => e2
      case (_, Literal(0.0, DoubleType)) => zero
      case (Literal(0.0, DoubleType), _) => zero
      case _ => e1 * e2
    }
  }

  def xeSuff(suffBaseUpdate: Seq[Expression], p: Int): Seq[Expression] = {
    def xr(i: Int, k: Int): Expression = suffBaseUpdate(OLS.ijToInd(i + 1, k, p + 1))

    OLS.lowerCross(p, (i, j) => {
      OLS.lowerCross(p + 1,
        (ik, jk) =>
          if (ik == jk) {
            mulSimplify(xr(i, ik), xr(j, jk))
          } else {
            mulSimplify(xr(i, ik), xr(j, jk)) + mulSimplify(xr(i, jk), xr(j, ik))
          }
      )
    }).flatten
  }

  def getSuff(yx: Seq[Expression], stdErrorType: String, removeNaN: Expression): Seq[Expression] = {
    val p = yx.length - 1
    val suffBaseUpdate = OLS.lowerCross(p + 1, (i, j) => mulSimplify(yx(i), yx(j))) :+ one
    val litFalse: Expression = Literal(false)
    val hasNull = yx
      .filter {
        case Literal(v, _) if v == null => false
        case _ => true
      }.map(_.isNull).reduceOption(Or).getOrElse(litFalse)
    val hasNaN = yx
      .filter {
        case Literal(v: Double, _) if v.isNaN => false
        case Literal(v: Float, _) if v.isNaN => false
        case _ => true
      }.map(IsNaN).reduceOption(Or).getOrElse(litFalse)
    val rowRemoved = if (removeNaN == Literal(true)) hasNull || hasNaN else hasNull
    val updater = if (stdErrorType.toLowerCase(Locale.ROOT) == "const") {
      suffBaseUpdate
    } else {
      suffBaseUpdate ++ xeSuff(suffBaseUpdate, p)
    }
    updater.map(ei => If(rowRemoved, zero, ei))
  }

  def suffSize(p: Int, stdErrorType: String): Int = {
    val xtxSize = p * (p + 1) / 2
    val yxtxySize = (p + 1) * (p + 2) / 2
    val innerSqrSize = (p + 1) * (p + 2) / 2
    stdErrorType.toLowerCase(Locale.ROOT) match {
      case "const" => yxtxySize + 1
      case "hc0" => yxtxySize + 1 + xtxSize * innerSqrSize
      case "hc1" => yxtxySize + 1 + xtxSize * innerSqrSize + 1
    }
  }

  def ols(
    xtx: Array[Double], xty: Array[Double], yty: Double, n: Double
  ): (Array[Double], Array[Double], Array[Double]) = {
    val p = xty.length
    val _xtxM = new Array2DRowRealMatrix(p, p)
    for {
      i <- 0 until p
      j <- 0 until p
    } {
      val ind = if (i <= j) {
        i * p + j - i * (i + 1) / 2
      } else {
        j * p + i - j * (j + 1) / 2
      }
      _xtxM.setEntry(i, j, xtx(ind))
    }
    val _xtyM = new Array2DRowRealMatrix(xty)

    def getRes(xtxM: RealMatrix, xtyM: RealMatrix) = {
      val xtxInv = new RRQRDecomposition(xtxM).getSolver.getInverse
      val p = xtyM.getData.length
      val b = xtxInv.multiply(xtyM)
      val df = n - p
      val xtyb = xtyM.transpose().multiply(b).getEntry(0, 0)
      val bxtxb = b.transpose().multiply(xtxM).multiply(b).getEntry(0, 0)
      val residSqr = yty - 2.0 * xtyb + bxtxb
      val sigma = math.sqrt(residSqr / df)
      val stderr = Array.range(0, p).map { i =>
        math.sqrt(xtxInv.getEntry(i, i)) * sigma
      }
      val xtxInvArr = Array.range(0, p).flatMap(i =>
        Array.range(i, p).map(j => xtxInv.getEntry(i, j))
      )
      (b.getData.flatten, stderr, xtxInvArr)
    }

    val qr = new RRQRDecomposition(_xtxM)
    if (qr.getSolver.isNonSingular) {
      return getRes(_xtxM, _xtyM)
    }
    val rank = qr.getRank(1.0e-6)
    val pivot = Array.range(0, p).map { i => qr.getP.getRow(i).indexOf(1.0) }
    val slice = pivot.slice(0, rank).sorted
    val xtxM = _xtxM.getSubMatrix(slice, slice)
    val xtyM = _xtyM.getSubMatrix(slice, Array(0))
    val (_b, _stderr, xtxInvArr) = getRes(xtxM, xtyM)
    val slice2index = slice.zipWithIndex.toMap
    val (b, stderr) = Array.tabulate(rank) { i =>
      if (slice2index.contains(i)) {
        val ind = slice2index(i)
        (_b(ind), _stderr(ind))
      } else {
        (Double.NaN, Double.NaN)
      }
    }.unzip
    (b, stderr, xtxInvArr)
  }

  def lowerCross[A](p: Int, f: (Int, Int) => A): Seq[A] = {
    Seq.range(0, p).flatMap { i =>
      Seq.range(i, p).map { j =>
        f(i, j)
      }
    }
  }

  def ijToInd(i: Int, j: Int, p: Int): Int = {
    if (i <= j) {
      i * p + j - i * (i + 1) / 2
    } else {
      j * p + i - j * (j + 1) / 2
    }
  }

  def dataTypes(method: String, isFactor: Boolean): DataType = {
    val dt = StructType(Seq(
      StructField("delta", DoubleType, nullable = false),
      StructField("stderr", DoubleType, nullable = false),
      StructField("tvalue", DoubleType, nullable = false),
      StructField("pvalue", DoubleType, nullable = false),
      StructField("lower", DoubleType, nullable = false),
      StructField("upper", DoubleType, nullable = false)
    ))
    val (my, mx, my0, mx0) = (
      StructField("my", DoubleType, nullable = false),
      StructField("mx", DoubleType, nullable = false),
      StructField("my0", DoubleType, nullable = false),
      StructField("mx0", DoubleType, nullable = false),
    )
    method.toLowerCase(Locale.ROOT) match {
      case "ols" | "cluster_ols" => ArrayType(dt, containsNull = false)
      case "ttest" | "cluster_ttest" | "anova" | "cluster_anova" =>
        if (isFactor) {
          ArrayType(StructType(dt :+ my), containsNull = false)
        } else {
          StructType(dt :+ my0 :+ my)
        }
      case "ancova1" | "cluster_ancova1" | "ancova2" | "cluster_ancova2" =>
        if (isFactor) {
          ArrayType(StructType(dt :+ my :+ mx), containsNull = false)
        } else {
          StructType(dt :+ my0 :+ my :+ mx0 :+ mx)
        }
    }
  }

  def offset(method: String): Int = {
    val nonX = Seq("ols", "ttest", "anova", "cluster_ols", "cluster_ttest", "cluster_anova")
      .contains(method)
    if (nonX) 2 else 3
  }

  private val zero: Literal = Literal(0.0)
  private val one: Literal = Literal(1.0)

  def toX(children: Seq[Expression], method: String): Seq[Expression] = {
    val lowerMethod = method.toLowerCase(Locale.ROOT)
    if (lowerMethod == "ols" | lowerMethod == "cluster_ols") {
      val x = children(1)
      val X = x
        .collect { case e: CreateArray => e.children }
        .headOption.getOrElse(Seq())
      return X
    }

    val treat = Factor.getTreat(children(1), onehot = false)
    method.toLowerCase(Locale.ROOT) match {
      case "ttest" | "cluster_ttest" | "anova" | "cluster_anova" =>
        treat
      case "ancova1" | "cluster_ancova1" =>
        val x = children(2)
        treat :+ x
      case "ancova2" | "cluster_ancova2" =>
        val x = children(2)
        (treat :+ x) ++ treat.tail.map(_ * x)
    }
  }
}
