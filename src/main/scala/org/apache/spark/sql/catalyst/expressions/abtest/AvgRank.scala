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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.extra.{ExpressionUtils, FunctionDescription}
import org.apache.spark.sql.types._


/*
 * The AvgRank function computes the rank of a value in a group of values. When ties, return average
 * rank.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage =
    """
    _FUNC_() - Computes the rank of a value in a group of values. When ties, return average rank.
    """,
  arguments =
    """
    Arguments:
      * children - this is to base the rank on; a change in the value of one the children will
          trigger a change in rank. This is an internal parameter and will be assigned by the
          Analyser.
    """,
  examples =
    """
    Examples:
      > SELECT a, b, _FUNC_() OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	1.5
       A1	1	1.5
       A1	2	3.0
       A2	3	1.0
    """,
  since = "3.5.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class AvgRank(children: Seq[Expression]) extends RankLike {
  def this() = this(Nil)

  def withOrder(order: Seq[Expression]): AvgRank = AvgRank(order)

  // The frame for MAX_RANK is Range based instead of Row based, because MAX_RANK must
  // return the same value for equal values in the partition.
  override val frame: WindowFrame = SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)

  override val dataType: DataType = DoubleType
  override val evaluateExpression: Expression =
    (rank.cast(DoubleType) + rowNumber.cast(DoubleType)) / Literal(2.0) - Literal(0.5)

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): AvgRank =
    copy(children = newChildren)

  override def prettyName: String = "avg_rank"
}

object AvgRank {
  val fd: FunctionDescription = (
    new FunctionIdentifier("avg_rank"),
    ExpressionUtils.getExpressionInfo(classOf[AvgRank], "avg_rank"),
    (_: Seq[Expression]) => new AvgRank())
}
