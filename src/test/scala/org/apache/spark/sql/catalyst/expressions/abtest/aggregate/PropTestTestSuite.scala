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

import org.apache.spark.sql.Row
import org.apache.spark.sql.extra.SparkSessionHelper
import org.apache.spark.sql.functions.expr

class PropTestTestSuite extends SparkSessionHelper {
  /*
    ```R
    library(tidyverse)
    library(glue)
    get_prop_test_res = function(n, x, ...) {
      if (length(n) <= 2) {
        mod = prop.test(rev(x), rev(n), ...)
        print(mod)
        ret = c(mod$p.value, mod$conf.int[1], mod$conf.int[2], mod$statistic)
        ret = sprintf('%.6f', ret)
        names(ret) = c('p.value', 'lower', 'upper', 'statistic')
        ret = as.list(ret)
        m = paste0(sprintf('%.6f', x / n), collapse = ', ')
        glue("Row(Seq({m}), {ret$p.value}, {ret$lower}, {ret$upper}, {ret$statistic})")
      } else {
        mod = prop.test(x, n, ...)
        print(mod)
        ret = c(mod$p.value, mod$statistic)
        ret = sprintf('%.6f', ret)
        names(ret) = c('p.value', 'statistic')
        ret = as.list(ret)
        ret$lower = 'Double.NaN'
        ret$upper = 'Double.NaN'
        m = paste0(sprintf('%.6f', x / n), collapse = ', ')
        glue("Row(Seq({m}), {ret$p.value}, {ret$lower}, {ret$upper}, {ret$statistic})")
      }
    }
    ```
  * */

  test("nan/null") {
    val df2t = spark.sql(
      """select explode(transform(sequence(0, n - 1), i -> if(i < y, 1, 0))) as y, t
        |  from values (0, 10, 7), (1, 8, 4) as (t, n, y)
        |""".stripMargin)
    df2t.createOrReplaceTempView("df2t")

    // removeNaN is false and contains nan, result is null
    checkAnswer(
      df2t.union(spark.sql("select 1, double('nan')"))
        .agg(expr("prop_test(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(null))
    )

    checkAnswer(
      df2t.union(spark.sql("select double('nan'), 1"))
        .agg(expr("prop_test(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(null))
    )

    // row contains any null will always removed
    checkAnswer(
      df2t.union(spark.sql("select null, 1"))
        .agg(expr("prop_test(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(Row(Seq(0.700000, 0.500000), 0.387094, -0.648014, 0.248014, 0.748052)))
    )

    checkAnswer(
      df2t.union(spark.sql("select 1, null"))
        .agg(expr("prop_test(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(Row(Seq(0.700000, 0.500000), 0.387094, -0.648014, 0.248014, 0.748052)))
    )
  }

  test("two-group-prop-test") {
    val df2t = spark.sql(
      """select explode(transform(sequence(0, n - 1), i -> if(i < y, 1, 0))) as y, t
        |  from values (0, 10, 7), (1, 8, 4) as (t, n, y)
        |""".stripMargin)
    df2t.createOrReplaceTempView("df2t")

    // get_prop_test_res(c(10, 8), c(7, 4))
    checkAnswer(
      df2t.agg(expr("prop_test(y, t)")),
      Seq(Row(Row(Seq(0.700000, 0.500000), 0.705138, -0.760514, 0.360514, 0.143182)))
    )

    // get_prop_test_res(c(10, 8), c(7, 4), correct = FALSE)
    checkAnswer(
      df2t.agg(expr("prop_test(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(Row(Seq(0.700000, 0.500000), 0.387094, -0.648014, 0.248014, 0.748052)))
    )

    // get_prop_test_res(c(10, 8), c(7, 4), alternative = 'greater', conf.level = 0.90)
    checkAnswer(
      df2t.agg(expr("prop_test(y, t, 0.90, 'greater')")),
      Seq(Row(Row(Seq(0.700000, 0.500000), 0.647431, -0.605440, 1.000000, 0.143182)))
    )

    // get_prop_test_res(c(10, 8), c(7, 4), alternative = 'less', conf.level = 0.85)
    checkAnswer(
      df2t.agg(expr("prop_test(y, t, 0.85, 'less')")),
      Seq(Row(Row(Seq(0.700000, 0.500000), 0.352569, -1.000000, 0.149411, 0.143182)))
    )
  }

  test("factor-prop-test") {

    // get_prop_test_res(c(86, 93, 136, 82), c(83, 90, 129, 70))
    val df3t = spark.sql(
      """select t, explode(transform(sequence(0, n - 1), i -> if(i < y, 1, 0))) as y
        |  from values ('a', 86, 83), ('b', 93, 90), ('c', 136, 129), ('d', 82, 70) as (t, n, y)
        |""".stripMargin)
    df3t.createOrReplaceTempView("df3t")

    checkAnswer(
      df3t.agg(expr("prop_test(y, factor(t, array('a', 'b', 'c', 'd')))")),
      Seq(Row(Row(
        Seq(0.965116, 0.967742, 0.948529, 0.853659),
        0.005585, Double.NaN, Double.NaN, 12.600411
      )))
    )
  }

  test("corner-case") {
    checkAnswer(
      spark.sql("select prop_test(1, 1)"),
      Seq(Row(Row(Seq(1.0), 1.0, 0.054621, 0.892226, 0.0)))
    )
  }
}

