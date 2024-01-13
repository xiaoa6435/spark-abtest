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

class AvgRankTestSuite extends SparkSessionHelper {
  test("avg-rank") {
    checkAnswer(
      spark.sql(
        """
          |select k, v
          |       ,avg_rank(v) OVER (PARTITION BY k ORDER BY v) as avg_rk
          |  from values ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) as tab(k, v)
          | order by k, v
          |""".stripMargin),
      Seq(
        Row("A1", 1, 1.5),
        Row("A1", 1, 1.5),
        Row("A1", 2, 3.0),
        Row("A2", 3, 1.0)
      )
    )
  }
}
