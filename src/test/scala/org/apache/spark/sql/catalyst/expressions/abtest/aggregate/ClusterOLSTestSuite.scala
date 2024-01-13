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

class ClusterOLSTestSuite extends SparkSessionHelper {

  test("corner-case") {
    checkAnswer(
      spark.sql("select cluster_ols(null, array(1.0), double('nan'))"),
      Seq(Row(null))
    )
  }

  test("nan/null") {
    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x", "cid")
        .union(spark.sql("select null, 1.0, 2.2, 1.0"))
        .select(expr("inline(cluster_ols(y, array(1.0, t, x), cid))")),
      Seq(
        Row(1.777724, 0.050830, 34.974183, 0.000000, 1.677784, 1.877664),
        Row(0.275974, 0.107329, 2.571291, 0.010509, 0.064946, 0.487003),
        Row(0.026428, 0.008068, 3.275479, 0.001151, 0.010564, 0.042291)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x", "cid")
        .union(spark.sql("select 1.0, null, 2.2, 1.0"))
        .select(expr("inline(cluster_ols(y, array(1.0, t, x), cid))")),
      Seq(
        Row(1.777724, 0.050830, 34.974183, 0.000000, 1.677784, 1.877664),
        Row(0.275974, 0.107329, 2.571291, 0.010509, 0.064946, 0.487003),
        Row(0.026428, 0.008068, 3.275479, 0.001151, 0.010564, 0.042291)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x", "cid")
        .union(spark.sql("select double('nan'), 1.0, 2.2, 1.0"))
        .select(expr("inline(cluster_ols(y, array(1.0, t, x), cid))")),
      Seq(
        Row(1.777724, 0.050830, 34.974183, 0.000000, 1.677784, 1.877664),
        Row(0.275974, 0.107329, 2.571291, 0.010509, 0.064946, 0.487003),
        Row(0.026428, 0.008068, 3.275479, 0.001151, 0.010564, 0.042291)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x", "cid")
        .union(spark.sql("select 1.0, double('nan'), 2.2, 1.0"))
        .select(expr("inline(cluster_ols(y, array(1.0, t, x), cid))")),
      Seq(
        Row(1.777724, 0.050830, 34.974183, 0.000000, 1.677784, 1.877664),
        Row(0.275974, 0.107329, 2.571291, 0.010509, 0.064946, 0.487003),
        Row(0.026428, 0.008068, 3.275479, 0.001151, 0.010564, 0.042291)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x", "cid")
        .union(spark.sql("select 1.0, double('nan'), 2.2, 1.0"))
        .select(expr("cluster_ols(y, array(1.0, t, x), cid, 0.95, 'HC1', false)")),
      Seq(Row(null))
    )
  }

  test("cluster-ols") {

    // get_res(ols = lm(y ~ t + x, sdf), is_cluster = TRUE)
    checkAnswer(
      spark.sql("select inline(cluster_ols(y, array(1.0, t, x), cid)) from sdf"),
      Seq(
        Row(1.777724, 0.050830, 34.974183, 0.000000, 1.677784, 1.877664),
        Row(0.275974, 0.107329, 2.571291, 0.010509, 0.064946, 0.487003),
        Row(0.026428, 0.008068, 3.275479, 0.001151, 0.010564, 0.042291)
      )
    )

    // get_res(ols = lm(y ~ t + x + t * x, sdf), is_cluster = TRUE, confLevel = 0.90)
    checkAnswer(
      spark.sql(
        """
          |select inline(cluster_ols(y, array(1.0, t, x, t * x), cid, 0.90, 'HC1')) from sdf
          |""".stripMargin),
      Seq(
        Row(1.753617, 0.055801, 31.426090, 0.000000, 1.661608, 1.845625),
        Row(0.354508, 0.138141, 2.566271, 0.010660, 0.126734, 0.582283),
        Row(0.033937, 0.011462, 2.960763, 0.003260, 0.015038, 0.052837),
        Row(-0.018718, 0.014059, -1.331447, 0.183836, -0.041899, 0.004462)
      )
    )

    // get_res(ols = lm(y ~ x, sdf), is_cluster = TRUE, confLevel = 0.90, stdErrorType = 'HC0')
    checkAnswer(
      spark.sql("select inline(cluster_ols(y, array(1.0, x), cid, 0.90, 'HC0')) from sdf"),
      Seq(
        Row(1.825670, 0.055149, 33.104597, 0.000000, 1.734740, 1.916601),
        Row(0.032903, 0.007516, 4.378058, 0.000015, 0.020512, 0.045295)
      )
    )
  }


  test("cluster-anova/ancova1/ancova2") {

    // get_res(type = 'anova', is_cluster = TRUE)
    checkAnswer(
      spark.sql("select cluster_anova(y, t, cid) from sdf"),
      Seq(Row(Row(0.319468, 0.098263, 3.251140, 0.001251, 0.126266, 0.512669, 1.862558, 2.182026)))
    )

    // get_res(type = 'ancova1', is_cluster = TRUE)
    checkAnswer(
      spark.sql("select cluster_ancova1(y, t, x, cid) from sdf"),
      Seq(Row(Row(
        0.275974, 0.107329, 2.571291, 0.010509, 0.064946, 0.487003,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova2', is_cluster = TRUE, stdErrorType = 'HC0', confLevel = 0.80)
    checkAnswer(
      spark.sql("select cluster_ancova2(y, t, x, cid, 0.80, 'hc0') from sdf"),
      Seq(Row(Row(
        0.286440, 0.106845, 2.680900, 0.007661, 0.149276, 0.423604,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    // get_res(type = 'ancova2', is_cluster = TRUE, stdErrorType = 'HC0', confLevel = 0.80)
    checkAnswer(
      spark.sql("select cluster_ancova2(y, t, x, cid, 0.80, 'hc0') from sdf"),
      Seq(Row(Row(
        0.286440, 0.106845, 2.680900, 0.007661, 0.149276, 0.423604,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x", "cid")
        .union(spark.sql("select 1.0, double('nan'), 2.2, 1.0"))
        .select(expr("cluster_ancova2(y, t, x, cid, 0.80, 'hc0')")),
      Seq(Row(Row(
        0.286440, 0.106845, 2.680900, 0.007661, 0.149276, 0.423604,
        1.862558, 2.182026, 3.210082, 4.855833
      )))
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "t", "x", "cid")
        .union(spark.sql("select 1.0, double('nan'), 2.2, 1.0"))
        .select(expr("cluster_ancova2(y, t, x, cid, 0.80, 'hc0', false)")),
      Seq(Row(null))
    )
  }

  test("factor-cluster-anova/ancova1/ancova2") {
    // get_res(type = 'anova', is_cluster = TRUE, is_factor = TRUE)
    checkAnswer(
      spark.sql(
        """
          |select inline(cluster_anova(y, factor(mt3, array('t0', 't1', 't2')), cid)) from sdf
          |""".stripMargin),
      Seq(
        Row(1.965583, 0.077635, 25.318245, 0.000000, 1.812938, 2.118227, 1.965583),
        Row(-0.033284, 0.104824, -0.317524, 0.751019, -0.239386, 0.172818, 1.932298),
        Row(-0.031814, 0.118140, -0.269289, 0.787853, -0.264098, 0.200471, 1.933769)
      )
    )

    // get_res(type = 'ancova1', is_cluster = TRUE, is_factor = TRUE)
    checkAnswer(
      spark.sql(
        """
          |select inline(cluster_ancova1(y, factor(mt3, array('t0', 't1', 't2')), x, cid)) from sdf
          |""".stripMargin),
      Seq(
        Row(1.803655, 0.091732, 19.662205, 0.000000, 1.623292, 1.984018, 1.965583, 4.750571),
        Row(0.014212, 0.104193, 0.136401, 0.891576, -0.190652, 0.219076, 1.932298, 3.357143),
        Row(0.044276, 0.123945, 0.357221, 0.721124, -0.199424, 0.287976, 1.933769, 2.518275)
      )
    )

    // get_res(type = 'ancova2', is_cluster = TRUE, is_factor = TRUE)
    checkAnswer(
      spark.sql(
        """
          |select inline(cluster_ancova2(y, factor(mt3, array('t0', 't1', 't2')), x, cid)) from sdf
          |""".stripMargin),
      Seq(
        Row(1.932657, 0.081513, 23.709939, 0.000000, 1.772385, 2.092929, 1.965583, 4.750571),
        Row(0.013353, 0.103903, 0.128515, 0.897810, -0.190944, 0.217650, 1.932298, 3.357143),
        Row(0.024890, 0.128437, 0.193793, 0.846441, -0.227647, 0.277427, 1.933769, 2.518275)
      )
    )

    // scalastyle:off line.size.limit line.contains.tab
    // get_res(type = 'ancova2', is_cluster = TRUE, is_factor = TRUE, stdErrorType = 'HC0', confLevel = 0.80)
    checkAnswer(
      spark.sql(
        """
          |select inline(cluster_ancova2(y, factor(mt3, array('t0', 't1', 't2')), x, cid, '0.80', 'hc0')) from sdf
          |""".stripMargin),
      Seq(
        Row(1.932657, 0.080486, 24.012282, 0.000000, 1.829330, 2.035983, 1.965583, 4.750571),
        Row(0.013353, 0.102595, 0.130154, 0.896514, -0.118356, 0.145063, 1.932298, 3.357143),
        Row(0.024890, 0.126820, 0.196265, 0.844508, -0.137919, 0.187700, 1.933769, 2.518275)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "mt3", "x", "cid")
        .union(spark.sql("select 't0', double('nan'), 2.2, 1.0"))
        .select(expr("inline(cluster_ancova2(y, factor(mt3, array('t0', 't1', 't2')), x, cid, '0.80', 'hc0'))")),
      Seq(
        Row(1.932657, 0.080486, 24.012282, 0.000000, 1.829330, 2.035983, 1.965583, 4.750571),
        Row(0.013353, 0.102595, 0.130154, 0.896514, -0.118356, 0.145063, 1.932298, 3.357143),
        Row(0.024890, 0.126820, 0.196265, 0.844508, -0.137919, 0.187700, 1.933769, 2.518275)
      )
    )

    checkAnswer(
      spark.table("sdf")
        .select("y", "mt3", "x", "cid")
        .union(spark.sql("select 1.0, 2.2, double('nan'), 1.0"))
        .select(expr("cluster_ancova2(y, factor(mt3, array('t0', 't1', 't2')), x, cid, '0.80', 'hc0', false)")),
      Seq(Row(null))
    )
    // scalastyle:on line.size.limit line.contains.tab

  }
}
