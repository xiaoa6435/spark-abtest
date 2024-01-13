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

class OLSTestSuite extends SparkSessionHelper {

  /*
  ```R
  library(tidyverse)
  library(lmtest)
  library(sandwich)
  library(glue)

  filepath = "src/test/resources/nlswork-samp.csv"
  sdf = read_csv(filepath, show_col_types = FALSE, comment = "#")
  sdf$xdm = sdf$x - mean(sdf$x)
  sdf$mt3 = paste0('t', sdf$cid %% 3)

  get_res = function(type = 'anova', is_cluster = FALSE, is_factor = FALSE,
    confLevel = 0.95, stdErrorType = "HC1", ols = NULL) {
    print(glue("type: {type}, is_cluster: {is_cluster}, is_factor: {is_factor}"))
    print(glue("confLevel: {confLevel}, stdErrorType: {stdErrorType}"))
    writeLines("\n")

    if (!is.null(ols)) {
      mod = ols
    } else if (is_factor && type == 'anova') {
      mod = lm(y ~ mt3, sdf)
    } else if (is_factor && type == 'ancova1') {
      mod = lm(y ~ mt3 + x, sdf)
    } else if (is_factor && type == 'ancova2') {
      mod = lm(y ~ mt3 * xdm, sdf)
    } else if (type == 'anova') {
      mod = lm(y ~ t, sdf)
    } else if (type == 'ancova1') {
      mod = lm(y ~ t + x, sdf)
    } else if (type == 'ancova2') {
      mod = lm(y ~ t * xdm, sdf)
    }
    vcov = if (is_cluster) {
      cadjust = stdErrorType == "HC1"
      vcovCL(mod, type = stdErrorType, cluster = ~cid, cadjust = cadjust)
    } else {
      vcovHC(mod, type = stdErrorType)
    }
    coef = coeftest(mod, vcov = vcov)
    ci = coefci(mod, vcov = vcov, level = confLevel)
    coef_ci = cbind(coef, ci)
    print(coef_ci)
    writeLines("\n")

    colnames(coef_ci) = c('delta', 'stderr', 'tvalue', 'pvalue', 'lower', 'upper')

    if (!is.null(ols)) {
      ret = as.data.frame(coef_ci)
    } else if (is_factor) {
      myx = sdf |>
        group_by(mt3) |>
        summarise(
          my = mean(y),
          mx = mean(x),
        ) |>
        select(-mt3)
      nt = nrow(myx)
      ret = as.data.frame(cbind(coef_ci[1:nt, , drop = FALSE], myx))
    } else {
      ret = coef_ci[2, , drop = FALSE]
      y0 = mean(sdf$y[sdf$t == 0])
      y1 = mean(sdf$y[sdf$t == 1])
      x0 = mean(sdf$x[sdf$t == 0])
      x1 = mean(sdf$x[sdf$t == 1])
      ret = as.data.frame(cbind(ret, y0, y1, x0, x1))
    }
    fmt = "Row({delta}, {stderr}, {tvalue}, {pvalue}, {lower}, {upper}"

    if (!is.null(ols)) {
      fmt = paste0(fmt, ")")
    } else if (!is_factor && type == 'anova') {
      fmt = paste0(fmt, ", {y0}, {y1})")
    } else if (!is_factor) {
      fmt = paste0(fmt, ", {y0}, {y1}, {x0}, {x1})")
    } else if (type == 'anova') {
      fmt = paste0(fmt, ", {my})")
    } else {
      fmt = paste0(fmt, ", {my}, {mx})")
    }
    ret |>
      mutate(across(is.numeric, ~sprintf("%.6f", .))) |>
      glue::glue_data(fmt) |>
      str_c(collapse = ",\n") |>
      writeLines()

    writeLines("\n")

    ret
  }
  ```
  */

  test("corner-case") {
    checkAnswer(
      spark.sql("select ols(y, array(x), 0.95d)[0] from values (0.0d, 1.0d) as df(y, x)"),
      Seq(Row(null))
    )

    checkAnswer(
      spark.sql("select ols(y, array(x))[0] from values (double('nan'), 1.0d) as df(y, x)"),
      Seq(Row(null))
    )

    checkAnswer(
      spark.sql("select ols(y, array(x))[0] from values (null, double('nan')) as df(y, x)"),
      Seq(Row(null))
    )
  }


  test("nan/null") {
    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select float('nan'), 1.0, 2.2"))
        .select(expr("inline(ols(y, array(1.0, t, x)))")),
      Seq(
        Row(1.777724, 0.031217, 56.946967, 0.000000, 1.716345, 1.839102),
        Row(0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513),
        Row(0.026428, 0.005924, 4.461404, 0.000011, 0.014781, 0.038074)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, float('nan'), 2.2"))
        .select(expr("inline(ols(y, array(1.0, t, x)))")),
      Seq(
        Row(1.777724, 0.031217, 56.946967, 0.000000, 1.716345, 1.839102),
        Row(0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513),
        Row(0.026428, 0.005924, 4.461404, 0.000011, 0.014781, 0.038074)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 2.2, float('nan')"))
        .select(expr("inline(ols(y, array(1.0, t, x)))")),
      Seq(
        Row(1.777724, 0.031217, 56.946967, 0.000000, 1.716345, 1.839102),
        Row(0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513),
        Row(0.026428, 0.005924, 4.461404, 0.000011, 0.014781, 0.038074)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 1.0, float('nan')"))
        .select(expr("ols(y, array(1.0, t, x), 0.95, 'const', false)")),
      Seq(Row(null))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select null, 1.0, 1.0"))
        .select(expr("inline(ols(y, array(1.0, t, x)))")),
      Seq(
        Row(1.777724, 0.031217, 56.946967, 0.000000, 1.716345, 1.839102),
        Row(0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513),
        Row(0.026428, 0.005924, 4.461404, 0.000011, 0.014781, 0.038074)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, null, 2.2"))
        .select(expr("inline(ols(y, array(1.0, t, x)))")),
      Seq(
        Row(1.777724, 0.031217, 56.946967, 0.000000, 1.716345, 1.839102),
        Row(0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513),
        Row(0.026428, 0.005924, 4.461404, 0.000011, 0.014781, 0.038074)
      )
    )
  }


  test("ols") {

    // get_res(ols = lm(y ~ t + x, sdf), stdErrorType = 'const')
    checkAnswer(
      spark.sql("select inline(ols(y, array(1.0, t, x))) from sdf"),
      Seq(
        Row(1.777724, 0.031217, 56.946967, 0.000000, 1.716345, 1.839102),
        Row(0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513),
        Row(0.026428, 0.005924, 4.461404, 0.000011, 0.014781, 0.038074)
      )
    )

    // get_res(ols = lm(y ~ t * x, sdf), stdErrorType = 'HC1', confLevel = 0.90)
    checkAnswer(
      spark.sql("select inline(ols(y, array(1.0, t, x, t * x), 0.90, 'HC1')) from sdf"),
      Seq(
        Row(1.753617, 0.035523, 49.366222, 0.000000, 1.695045, 1.812188),
        Row(0.354508, 0.077605, 4.568085, 0.000007, 0.226548, 0.482468),
        Row(0.033937, 0.008314, 4.081917, 0.000054, 0.020229, 0.047646),
        Row(-0.018718, 0.010458, -1.789793, 0.074279, -0.035963, -0.001474)
      )
    )

    /*
      ```R
      get_res(ols = lm(y ~ x, sdf), confLevel = 0.90, stdErrorType = 'HC0')
      ```
    * */
    checkAnswer(
      spark.sql("select inline(ols(y, array(1.0, x), 0.90, 'HC0')) from sdf"),
      Seq(
        Row(1.825670, 0.030861, 59.156932, 0.000000, 1.774785, 1.876556),
        Row(0.032903, 0.005179, 6.353789, 0.000000, 0.024365, 0.041442)
      )
    )
  }

  test("anova") {
    // get_res(type = 'anova', stdErrorType = 'const')
    checkAnswer(
      spark.sql("select anova(y, t) from sdf"),
      Seq(Row(Row(0.319468, 0.049824, 6.411972, 0.000000, 0.221506, 0.417429, 1.862558, 2.182026)))
    )

    // get_res(type = 'anova', stdErrorType = 'const', confLevel = 0.90)
    checkAnswer(
      spark.sql("select anova(y, t, 0.90) from sdf"),
      Seq(Row(Row(0.319468, 0.049824, 6.411972, 0.000000, 0.237317, 0.401618, 1.862558, 2.182026)))
    )

    // get_res(type = 'anova', stdErrorType = 'HC0')
    checkAnswer(
      spark.sql("select anova(y, t, 0.95, 'HC0') from sdf"),
      Seq(Row(Row(0.319468, 0.051643, 6.186050, 0.000000, 0.217929, 0.421007, 1.862558, 2.182026)))
    )

    // get_res(type = 'anova', stdErrorType = 'HC1')
    checkAnswer(
      spark.sql("select anova(y, t, 0.95, 'hc1') from sdf"),
      Seq(Row(Row(0.319468, 0.051778, 6.170003, 0.000000, 0.217665, 0.421271, 1.862558, 2.182026)))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t")
        .union(spark.sql("select null, 1.0"))
        .select(expr("anova(y, t)")),
      Seq(Row(Row(0.319468, 0.049824, 6.411972, 0.000000, 0.221506, 0.417429, 1.862558, 2.182026)))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t")
        .union(spark.sql("select 1.0, null"))
        .select(expr("anova(y, t)")),
      Seq(Row(Row(0.319468, 0.049824, 6.411972, 0.000000, 0.221506, 0.417429, 1.862558, 2.182026)))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t")
        .union(spark.sql("select double('nan'), 0.0"))
        .select(expr("anova(y, t)")),
      Seq(Row(Row(0.319468, 0.049824, 6.411972, 0.000000, 0.221506, 0.417429, 1.862558, 2.182026)))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t")
        .union(spark.sql("select 0.0, double('nan')"))
        .select(expr("anova(y, t)")),
      Seq(Row(Row(0.319468, 0.049824, 6.411972, 0.000000, 0.221506, 0.417429, 1.862558, 2.182026)))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t")
        .union(spark.sql("select 0.0, double('nan')"))
        .select(expr("anova(y, t, 0.95, 'const', false)")),
      Seq(Row(null))
    )
  }

  test("ancova1") {
    // get_res(type = 'ancova1', stdErrorType = 'const')
    checkAnswer(
      spark.sql("select ancova1(y, t, x) from sdf"),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova1', stdErrorType = 'const', confLevel = 0.90)
    checkAnswer(
      spark.sql("select ancova1(y, t, x, 0.90) from sdf"),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.194179, 0.357770,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova1', stdErrorType = 'HC0')
    checkAnswer(
      spark.sql("select ancova1(y, t, x, 0.95, 'HC0') from sdf"),
      Seq(Row(Row(
        0.275974, 0.054094, 5.101716, 0.000001, 0.169615, 0.382334,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova1', stdErrorType = 'HC1')
    checkAnswer(
      spark.sql("select ancova1(y, t, x, 0.95, 'hc1') from sdf"),
      Seq(Row(Row(
        0.275974, 0.054306, 5.081852, 0.000001, 0.169199, 0.382749,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select null, 1.0, 0.0"))
        .select(expr("ancova1(y, t, x)")),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, null, 0.0"))
        .select(expr("ancova1(y, t, x)")),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 1.0, null"))
        .select(expr("ancova1(y, t, x)")),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )


    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select double('nan'), 1.0, 0.0"))
        .select(expr("ancova1(y, t, x)")),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, double('nan'), 0.0"))
        .select(expr("ancova1(y, t, x)")),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 1.0, double('nan')"))
        .select(expr("ancova1(y, t, x)")),
      Seq(Row(Row(
        0.275974, 0.049608, 5.563098, 0.000000, 0.178436, 0.373513,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 1.0, double('nan')"))
        .select(expr("ancova1(y, t, x, 0.95, 'const', false)")),
      Seq(Row(null))
    )
  }

  test("ancova2") {
    // get_res(type = 'ancova2', stdErrorType = 'const')
    checkAnswer(
      spark.sql("select ancova2(y, t, x) from sdf"),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.188181, 0.384699,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova2', stdErrorType = 'const', confLevel = 0.90)
    checkAnswer(
      spark.sql("select ancova2(y, t, x, 0.90) from sdf"),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.204040, 0.368841,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova2', stdErrorType = 'HC0')
    checkAnswer(
      spark.sql("select ancova2(y, t, x, 0.95, 'HC0') from sdf"),
      Seq(Row(Row(
        0.286440, 0.055647, 5.147479, 0.000000, 0.177028, 0.395852,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova2', stdErrorType = 'HC1')
    checkAnswer(
      spark.sql("select ancova2(y, t, x, 0.95, 'hc1') from sdf"),
      Seq(Row(Row(
        0.286440, 0.055937, 5.120738, 0.000000, 0.176457, 0.396424,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select null, 1.0, 0.0"))
        .select(expr("ancova2(y, t, x)")),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.188181, 0.384699,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, null, 0.0"))
        .select(expr("ancova2(y, t, x)")),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.188181, 0.384699,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 1.0, null"))
        .select(expr("ancova2(y, t, x)")),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.188181, 0.384699,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )


    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select double('nan'), 1.0, 0.0"))
        .select(expr("ancova2(y, t, x)")),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.188181, 0.384699,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, double('nan'), 0.0"))
        .select(expr("ancova2(y, t, x)")),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.188181, 0.384699,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 1.0, double('nan')"))
        .select(expr("ancova2(y, t, x)")),
      Seq(Row(Row(
        0.286440, 0.049974, 5.731740, 0.000000, 0.188181, 0.384699,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x")
        .union(spark.sql("select 1.0, 1.0, double('nan')"))
        .select(expr("ancova2(y, t, x, 0.95, 'const', false)")),
      Seq(Row(null))
    )
  }

  test("factor-anova/ancova1/ancova2") {

    // get_res(is_factor = TRUE, type = 'anova', stdErrorType = 'const')
    checkAnswer(
      spark.sql("select inline(anova(y, factor(mt3, array('t0', 't1', 't2')))) from sdf"),
      Seq(
        Row(1.965583, 0.037371, 52.596665, 0.000000, 1.892105, 2.039060, 1.965583),
        Row(-0.033284, 0.054908, -0.606183, 0.544753, -0.141242, 0.074674, 1.932298),
        Row(-0.031814, 0.056437, -0.563701, 0.573288, -0.142780, 0.079152, 1.933769)
      )
    )

    // get_res(is_factor = TRUE, type = 'ancova1', stdErrorType = 'HC1', confLevel = 0.80)
    checkAnswer(
      spark.sql(
        """
          |select inline(ancova1(y, factor(mt3, array('t0', 't1', 't2')), x, 0.80, 'hc1'))
          |  from sdf
          |""".stripMargin),
      Seq(
        Row(1.803655, 0.044911, 40.160589, 0.000000, 1.746000, 1.861311, 1.965583, 4.750571),
        Row(0.014212, 0.051904, 0.273815, 0.784375, -0.052421, 0.080845, 1.932298, 3.357143),
        Row(0.044276, 0.056945, 0.777516, 0.437335, -0.028829, 0.117380, 1.933769, 2.518275)
      )
    )

    // get_res(is_factor = TRUE, type = 'ancova2', stdErrorType = 'HC0', confLevel = 0.90)
    checkAnswer(
      spark.sql(
        """
          |select inline(ancova2(y, factor(mt3, array('t0', 't1', 't2')), x, 0.90, 'HC0'))
          |  from sdf
          |""".stripMargin),
      Seq(
        Row(1.932657, 0.036969, 52.277062, 0.000000, 1.871699, 1.993615, 1.965583, 4.750571),
        Row(0.013353, 0.051709, 0.258236, 0.796365, -0.071908, 0.098614, 1.932298, 3.357143),
        Row(0.024890, 0.058770, 0.423524, 0.672153, -0.072013, 0.121794, 1.933769, 2.518275)
      )
    )
  }
}

