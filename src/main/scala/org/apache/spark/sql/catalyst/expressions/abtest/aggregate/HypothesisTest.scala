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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait HypothesisTest extends DeclarativeAggregate with ImplicitCastInputTypes {

  val zero: Literal = Literal(0.0)
  val one: Literal = Literal(1.0)
  val suff: Seq[AttributeReference] = Nil

  override lazy val initialValues: Seq[Expression] = suff.map(_ => zero)
  override lazy val aggBufferAttributes: Seq[AttributeReference] = suff

  override lazy val updateExpressions: Seq[Expression] = Nil

  override lazy val mergeExpressions: Seq[Expression] = suff.map(e => e.left + e.right)


  override lazy val evaluateExpression: Expression = CreateArray(suff)

  override def nullable: Boolean = true

  override def dataType: DataType = evaluateExpression.dataType

  val y: Expression
  val confLevel: Expression = Literal(0.95)
  val alternative: Expression = Literal("two-sided")
  val stdErrorType: Expression = Literal("const")
  val varEqual: Expression = Literal(false)
  val removeNaN: Expression = Literal(true)


  // Mark as lazy so that is not evaluated during tree transformation.
  private lazy val _confLevel: Double = confLevel.eval().asInstanceOf[Double]
  lazy val _alternative: String = alternative.eval().asInstanceOf[UTF8String].toString
  lazy val _stdErrorType: String = stdErrorType.eval().asInstanceOf[UTF8String]
    .toString.toLowerCase(Locale.ROOT)

  def checkParaTypes(): TypeCheckResult = {
    if (!confLevel.foldable) {
      TypeCheckFailure(s"The confLevel must be a constant literal, but got $confLevel")
    } else if (confLevel.eval() == null) {
      TypeCheckFailure("confLevel value must not be null")
    } else if (_confLevel <= 0.0 || _confLevel >= 1.0) {
      TypeCheckFailure(s"confLevel must be between 0.0 and 1.0, but got $confLevel")
    } else if (!alternative.foldable) {
      TypeCheckFailure(s"The alternative must be a constant literal, but got $alternative")
    } else if (alternative.eval() == null) {
      TypeCheckFailure("alternative value must not be null")
    } else if (!List("two-sided", "less", "greater").contains(_alternative)) {
      TypeCheckFailure("alternative value must be [two-sided, less, greater]")
    } else if (!stdErrorType.foldable) {
      TypeCheckFailure(s"The varEqual must be a constant literal, but got $stdErrorType")
    } else if (stdErrorType.eval() == null) {
      TypeCheckFailure("stdErrorType value must not be null")
    } else if (!List("const", "hc0", "hc1", "hc2").contains(_stdErrorType)) {
      TypeCheckFailure("stdErrorType value must be [const, hc0, hc1, hc2]")
    } else if (varEqual.eval() == null) {
      TypeCheckFailure("varEqual value must not be null")
    } else if (removeNaN.eval() == null) {
      TypeCheckFailure("removeNaN value must not be null")
    } else {
      TypeCheckSuccess
    }
  }
}
