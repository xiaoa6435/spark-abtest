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

package org.apache.spark.sql.extra

import java.util.TimeZone

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.functions.expr
import org.apache.spark.util.Utils

trait SparkSessionHelper extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  protected var spark: SparkSession = _

  protected lazy val sql: String => DataFrame = spark.sql _

  override def beforeAll(): Unit = {
    val warehousePath = Utils.createTempDir()
    val metastorePath = Utils.createTempDir()
    warehousePath.delete()
    metastorePath.delete()
    spark = SparkSession.builder()
      .appName("test name")
      .master("local[3, 1]")
      .config("spark.sql.warehouse.dir", warehousePath.toString)
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metastorePath;create=true")
      .getOrCreate()
    new AbtestExtensions().apply(spark.extensions)

    val filepath = ClassLoader.getSystemResource("nlswork-samp.csv").getPath
    val sdf: DataFrame = spark.read
      .option("inferSchema", value = true)
      .option("header", value = true)
      .option("comment", "#")
      .csv(filepath)
      .withColumn("xdm", expr("x - avg(x) over ()"))
      .withColumn("mt3", expr("concat('t', pmod(cid, 3))"))
    sdf.createOrReplaceTempView("sdf")

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (spark != null) {
      spark.stop()
      spark = null
    }
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SessionState.detachSession()
    Hive.closeCurrent()
  }

  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer) match {
      case Some(errorMessage) => Assert.fail(errorMessage)
      case None =>
    }
  }

  def checkAnswer(sparkAnswer: Seq[Row], expectedAnswer: Seq[Row]): Unit = {
    sameRows(expectedAnswer, sparkAnswer).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  val tolerance: Double = 1e-6

  // copy from org.apache.spark.sql.catalyst.util
  def sideBySide(left: String, right: String): Seq[String] = {
    sideBySide(left.split("\n"), right.split("\n"))
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.length).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")
    leftPadded.zip(rightPadded).map {
      case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
    }
  }

  // copy from org.apache.spark.sql.QueryTest
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] => seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  private def compare(obj1: Any, obj2: Any, withTol: Boolean = true): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    case (a: Double, b: Double) if withTol =>
      if ((a.isNaN && b.isNaN) ||
        (a.isPosInfinity && b.isPosInfinity) ||
        (a.isNegInfinity && b.isNegInfinity)) {
        true
      } else {
        math.abs(a - b) <= tolerance
      }
    case (a: Float, b: Float) if withTol =>
      if ((a.isNaN && b.isNaN) ||
        (a.isPosInfinity && b.isPosInfinity) || (a.isNegInfinity && b.isNegInfinity)) {
        true
      } else {
        math.abs(a - b) <= tolerance
      }
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) =>
      java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
    case (a: Float, b: Float) =>
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a, b) => a == b
  }

  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  private def genError(
    expectedAnswer: Seq[Row],
    sparkAnswer: Seq[Row],
    isSorted: Boolean = false): String = {
    val getRowType: Option[Row] => String = row =>
      row.map(row =>
        if (row.schema == null) {
          "struct<>"
        } else {
          s"${row.schema.catalogString}"
        }).getOrElse("struct<>")

    s"""
       |== Results ==
       |${
      sideBySide(
        s"== Correct Answer - ${expectedAnswer.size} ==" +:
          getRowType(expectedAnswer.headOption) +:
          prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
        s"== Spark Answer - ${sparkAnswer.size} ==" +:
          getRowType(sparkAnswer.headOption) +:
          prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")
    }
     """.stripMargin
  }

  def sameRows(
    expectedAnswer: Seq[Row],
    sparkAnswer: Seq[Row],
    isSorted: Boolean = false): Option[String] = {
    if (!compare(prepareAnswer(expectedAnswer, isSorted), prepareAnswer(sparkAnswer, isSorted))) {
      return Some(genError(expectedAnswer, sparkAnswer, isSorted))
    }
    None
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a None will
   * be returned.
   *
   * @param df             the DataFrame to be executed
   * @param expectedAnswer the expected result in a Seq of Rows.
   * @param checkToRDD     whether to verify deserialization to an RDD. This runs the query twice.
   */
  def getErrorMessageInCheckAnswer(
    df: DataFrame,
    expectedAnswer: Seq[Row],
    checkToRDD: Boolean = true): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    if (checkToRDD) {
      SQLExecution.withSQLConfPropagated(df.sparkSession) {
        df.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
      }
    }

    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
           """.stripMargin
        return Some(errorMessage)
    }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |${df.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }
}
