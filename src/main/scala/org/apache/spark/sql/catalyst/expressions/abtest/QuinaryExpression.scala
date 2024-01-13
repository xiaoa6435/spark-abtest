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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.errors.QueryExecutionErrors


/*
 * An expression with five inputs and one output. The output is by default evaluated to null if
 * any input is evaluated to null.
 */
// noinspection ScalaWeakerAccess,ScalaUnusedSymbol
abstract class QuinaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of QuinaryExpression. If
   * subclass of QuinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            val v5 = exprs(4).eval(input)
            if (v5 != null) {
              return nullSafeEval(v1, v2, v3, v4, v5)
            }
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation. If subclass of QuinaryExpression keep the default
   * nullability, they can override this method to save null-check code. If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(
    input1: Any,
    input2: Any,
    input3: Any,
    input4: Any,
    input5: Any): Any = {
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(
      "QuinaryExpression",
      "eval",
      "nullSafeEval")
  }

  /**
   * Short hand for generating quinary evaluation code. If either of the sub-expressions is null,
   * the result of this computation is assumed to be null.
   *
   * @param f
   * accepts seven variable names and returns Java code to compute the output.
   */
  // noinspection ScalaUnusedSymbol
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String, String) => String): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (eval1, eval2, eval3, eval4, eval5) => {
        s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5)};"
      })
  }

  /**
   * Short hand for generating quinary evaluation code. If either of the sub-expressions is null,
   * the result of this computation is assumed to be null.
   *
   * @param f
   * function that accepts the 5 non-null evaluation result names of children and returns Java
   * code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String, String) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val resultCode =
      f(firstGen.value, secondGen.value, thirdGen.value, fourthGen.value, fifthGen.value)

    if (nullable) {
      val nullSafeEval =
        firstGen.code.toString + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code.toString + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code.toString + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code.toString + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                fifthGen.code.toString + ctx.nullSafeExec(children(4).nullable, fifthGen.isNull) {
                  s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                   """
                }
              }
            }
          }
        }

      ev.copy(code =
        code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(
        code =
          code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteral)
    }
  }
}

/*
 * An expression with six inputs + 7th optional input and one output.
 * The output is by default evaluated to null if any input is evaluated to null.
 */
abstract class SeptenaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of SeptenaryExpression.
   * If subclass of SeptenaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            val v5 = exprs(4).eval(input)
            if (v5 != null) {
              val v6 = exprs(5).eval(input)
              if (v6 != null) {
                if (exprs.length > 6) {
                  val v7 = exprs(6).eval(input)
                  if (v7 != null) {
                    return nullSafeEval(v1, v2, v3, v4, v5, v6, Some(v7))
                  }
                } else {
                  return nullSafeEval(v1, v2, v3, v4, v5, v6, None)
                }
              }
            }
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of SeptenaryExpression keep the
   * default nullability, they can override this method to save null-check code.  If we need
   * full control of evaluation process, we should override [[eval]].
   */
  // noinspection ScalaUnusedSymbol,ScalaWeakerAccess
  protected def nullSafeEval(
    input1: Any,
    input2: Any,
    input3: Any,
    input4: Any,
    input5: Any,
    input6: Any,
    input7: Option[Any]): Any = {
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("SeptenaryExpression",
      "eval", "nullSafeEval")
  }

  /**
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts seven variable names and returns Java code to compute the output.
   */
  // noinspection ScalaUnusedSymbol
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String, String, String, Option[String]) => String
  ): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4, eval5, eval6, eval7) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5, eval6, eval7)};"
    })
  }

  /**
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 7 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String, String, String, Option[String]) => String
  ): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val sixthGen = children(5).genCode(ctx)
    val seventhGen = if (children.length > 6) Some(children(6).genCode(ctx)) else None
    val resultCode = f(
      firstGen.value,
      secondGen.value,
      thirdGen.value,
      fourthGen.value,
      fifthGen.value,
      sixthGen.value,
      seventhGen.map(_.value))

    if (nullable) {
      val nullSafeEval =
        firstGen.code.toString + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code.toString + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code.toString + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code.toString + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                fifthGen.code.toString + ctx.nullSafeExec(children(4).nullable, fifthGen.isNull) {
                  sixthGen.code.toString + ctx.nullSafeExec(children(5).nullable, sixthGen.isNull) {
                    val nullSafeResultCode =
                      s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                       """
                    seventhGen.map { gen =>
                      gen.code.toString + ctx.nullSafeExec(children(6).nullable, gen.isNull) {
                        nullSafeResultCode
                      }
                    }.getOrElse(nullSafeResultCode)
                  }
                }
              }
            }
          }
        }

      ev.copy(code =
        code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code =
        code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${sixthGen.code}
        ${seventhGen.map(_.code).getOrElse("")}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}