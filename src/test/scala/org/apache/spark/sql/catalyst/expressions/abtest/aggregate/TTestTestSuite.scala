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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.extra.SparkSessionHelper
import org.apache.spark.sql.functions.expr

class TTestTestSuite extends SparkSessionHelper {

  /*
  ```R
  # set.seed(0)
  # N = 10
  # t = ifelse(runif(N) > 0.5, 1.0, 0.0)
  # y = runif(N)
  # df = data.frame(t = factor(t, levels = c(1, 0)), y) # set 1 as focal group
  # df$t |> paste(collapse = ", ")
  # df$y |> (\(x) sprintf('%.6f', x))() |> paste(collapse = ", ")

  library(glue)

  df = data.frame(
      t = factor(c(1, 0, 0, 1, 1, 0, 1, 1, 1, 1), levels = c(1, 0)), # set 1 as focal group
      y = c(0.061786, 0.205975, 0.176557, 0.687023, 0.384104, 0.769841, 0.497699, 0.717619, 0.991906, 0.380035)
   )
  get_res <- function(mod){
      res = c(
          mod$estimate[1] - mod$estimate[2], mod$stderr,
          mod$statistic, mod$p.value,
          mod$conf.int[1], mod$conf.int[2],
          mod$estimate[2], mod$estimate[1]
      ) |> (\(x) sprintf('%.6f', x))()
      glue('{paste0(res, collapse = ", ")}')
  }
  # purrr::map2_chr(t, y, \(t, y) paste0('(', t, 'd, ', sprintf('%.6f', y), 'd)')) |> paste(collapse = ", ")
  # Seq(
  #   (1d, 0.061786d), (0d, 0.205975d), (0d, 0.176557d), (1d, 0.687023d), (1d, 0.384104d),
  #   (0d, 0.769841d), (1d, 0.497699d), (1d, 0.717619d), (1d, 0.991906d), (1d, 0.380035d)
  # )
  ```
   */

  lazy val ttest_sdf: DataFrame = spark.sql(
    """
      |select y, t, t > 0.0 as tb
      |  from values
      |        (1d, 0.061786d), (0d, 0.205975d), (0d, 0.176557d), (1d, 0.687023d), (1d, 0.384104d),
      |        (0d, 0.769841d), (1d, 0.497699d), (1d, 0.717619d), (1d, 0.991906d), (1d, 0.380035d)
      |        as df(t, y)
      |""".stripMargin)

  test("ttest") {

    ttest_sdf.createOrReplaceTempView("ttest_sdf")

    // mod = t.test(y ~ t, df)
    // get_res(mod)
    checkAnswer(
      spark.sql("select ttest(y, t) from ttest_sdf"),
      Seq(Row(Row(0.147329, 0.223736, 0.658493, 0.551295, -0.512931, 0.807588, 0.384124, 0.531453)))
    )

    checkAnswer(
      spark.sql("select ttest(y, tb) from ttest_sdf"),
      Seq(Row(Row(0.147329, 0.223736, 0.658493, 0.551295, -0.512931, 0.807588, 0.384124, 0.531453)))
    )

    // mod = t.test(y ~ t, df, alternative = 'greater', conf.level = 0.90)
    // get_res(mod)
    checkAnswer(
      spark.sql("select ttest(y, t, 0.90, 'greater') from ttest_sdf"),
      Seq(Row(Row(
        0.147329, 0.223736, 0.658493, 0.275647,
        -0.206045, Double.PositiveInfinity, 0.384124, 0.531453
      )))
    )

    // mod = t.test(y ~ t, df, alternative = 'less', conf.level = 0.80, var.equal = FALSE)
    // get_res(mod)
    checkAnswer(
      spark.sql("select ttest(y, t, 0.80, 'less') from ttest_sdf"),
      Seq(Row(Row(
        0.147329, 0.223736, 0.658493, 0.724353,
        Double.NegativeInfinity, 0.361620, 0.384124, 0.531453
      )))
    )

    // mod = t.test(y ~ t, df, alternative = 'two.sided', conf.level = 0.95, var.equal = TRUE)
    // get_res(mod)
    checkAnswer(
      spark.sql("select ttest(y, t, 0.95, 'two-sided', true) from ttest_sdf"),
      Seq(Row(Row(0.147329, 0.212810, 0.692302, 0.508340, -0.343412, 0.638070, 0.384124, 0.531453)))
    )
  }

  test("nan/null") {
    checkAnswer(
      spark.sql("select ttest(1.0, 0.0)"),
      Seq(Row(null))
    )

    // removeNaN doesn't affect null: row contains null will always removed
    checkAnswer(
      ttest_sdf
        .union(spark.sql("select null, 1.0, true"))
        .select(expr("ttest(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(Row(0.147329, 0.223736, 0.658493, 0.551295, -0.512931, 0.807588, 0.384124, 0.531453)))
    )

    checkAnswer(
      ttest_sdf
        .union(spark.sql("select 1.0, null, true"))
        .select(expr("ttest(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(Row(0.147329, 0.223736, 0.658493, 0.551295, -0.512931, 0.807588, 0.384124, 0.531453)))
    )

    checkAnswer(
      ttest_sdf
        .union(spark.sql("select 1.0, double('nan'), true"))
        .select(expr("ttest(y, t, 0.95, 'two-sided', false, true)")),
      Seq(Row(Row(0.147329, 0.223736, 0.658493, 0.551295, -0.512931, 0.807588, 0.384124, 0.531453)))
    )

    checkAnswer(
      ttest_sdf
        .union(spark.sql("select double('nan'), 1.0, true"))
        .select(expr("ttest(y, t, 0.95, 'two-sided', false, true)")),
      Seq(Row(Row(0.147329, 0.223736, 0.658493, 0.551295, -0.512931, 0.807588, 0.384124, 0.531453)))
    )

    checkAnswer(
      ttest_sdf
        .union(spark.sql("select 1.0, double('nan'), true"))
        .agg(expr("ttest(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(null))
    )

    checkAnswer(
      ttest_sdf
        .union(spark.sql("select double('nan'), 1.0, true"))
        .agg(expr("ttest(y, t, 0.95, 'two-sided', false, false)")),
      Seq(Row(null))
    )
  }

  test("cuped") {

    /*
    ```R
    library(tidyverse)
    library(lmtest)
    library(sandwich)

    format_to_spark_res = function(confLevel = 0.95, alternative = 'two.sided'){
      theta = lm(y ~ x, sdf)$coefficients['x']
      sdf$ycv = sdf$y - sdf$x * theta
      sdf$t = factor(sdf$t, levels = c(1, 0))
      mod = t.test(ycv ~ t, sdf, conf.level = confLevel, alternative = alternative)
      delta = mod$estimate[1] - mod$estimate[2]
      y0 = mean(sdf$y[sdf$t == 0])
      y1 = mean(sdf$y[sdf$t == 1])
      x0 = mean(sdf$x[sdf$t == 0])
      x1 = mean(sdf$x[sdf$t == 1])
      ret = sprintf('%.6f', c(
          mod$estimate[1] - mod$estimate[2], mod$stderr,
          mod$statistic, mod$p.value,
          mod$conf.int[1], mod$conf.int[2],
          y0, y1, x0, x1, theta
      ))
      glue('{paste0(ret, collapse = ", ")}')
    }
    ```
    */

    // format_to_spark_res()
    checkAnswer(
      spark.sql("select cuped(y, t, x) from sdf"),
      Seq(Row(Row(
        0.265317, 0.051615, 5.140291, 0.000001, 0.163365, 0.367269,
        1.862558, 2.182026, 3.210082, 4.855833, 0.032903
      )))
    )

    // format_to_spark_res(confLevel = 0.90, alternative = 'greater')
    checkAnswer(
      spark.sql("select cuped(y, t, x, 0.90, 'greater') from sdf"),
      Seq(Row(Row(
        0.265317, 0.051615, 5.140291, 0.000000, 0.198889, Double.PositiveInfinity,
        1.862558, 2.182026, 3.210082, 4.855833, 0.032903
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, double('nan'), 1.0"))
        .select(expr("cuped(y, t, x, 0.90, 'greater', false, true)")),
      Seq(Row(Row(
        0.265317, 0.051615, 5.140291, 0.000000, 0.198889, Double.PositiveInfinity,
        1.862558, 2.182026, 3.210082, 4.855833, 0.032903
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, double('nan'), 1.0"))
        .select(expr("cuped(y, t, x, 0.90, 'greater', false, false)")),
      Seq(Row(null))
    )
  }
}
