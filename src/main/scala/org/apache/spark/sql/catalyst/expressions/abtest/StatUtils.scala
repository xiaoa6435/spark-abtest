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

import org.apache.commons.math3.distribution.{ChiSquaredDistribution, NormalDistribution, TDistribution}

import org.apache.spark.sql.catalyst.util.GenericArrayData

object StatUtils extends Serializable {
  def pnorm(x: Double): Double = {
    new NormalDistribution(0.0, 1.0).cumulativeProbability(x)
  }

  def qnorm(x: Double): Double = {
    new NormalDistribution(0.0, 1.0).inverseCumulativeProbability(x)
  }

  // noinspection ScalaUnusedSymbol
  def dnorm(x: Double): Double = {
    new NormalDistribution(0.0, 1.0).density(x)
  }

  def pt(x: Double, degreesOfFreedom: Double): Double = {
    // from https://github.com/wch/r-source/blob/trunk/src/nmath/pt.c
    if (degreesOfFreedom > 4e5) {
      new NormalDistribution(0.0, 1.0).cumulativeProbability(x)
    } else {
      new TDistribution(degreesOfFreedom).cumulativeProbability(x)
    }
  }

  def qt(x: Double, degreesOfFreedom: Double): Double = {
    // from https://github.com/wch/r-source/blob/trunk/src/nmath/pt.c
    if (degreesOfFreedom > 4e5) {
      new NormalDistribution(0.0, 1.0).inverseCumulativeProbability(x)
    } else {
      new TDistribution(degreesOfFreedom).inverseCumulativeProbability(x)
    }
  }

  def dt(x: Double, degreesOfFreedom: Double): Double = {
    // from https://github.com/wch/r-source/blob/trunk/src/nmath/pt.c
    if (degreesOfFreedom > 4e5) {
      new NormalDistribution(0.0, 1.0).density(x)
    } else {
      new TDistribution(degreesOfFreedom).density(x)
    }
  }

  def pchisq(x: Double, degreesOfFreedom: Double): Double = {
    new ChiSquaredDistribution(degreesOfFreedom).cumulativeProbability(x)
  }

  // noinspection ScalaUnusedSymbol
  def qchisq(x: Double, degreesOfFreedom: Double): Double = {
    new ChiSquaredDistribution(degreesOfFreedom).inverseCumulativeProbability(x)
  }

  // noinspection ScalaUnusedSymbol
  def dchisq(x: Double, degreesOfFreedom: Double): Double = {
    new ChiSquaredDistribution(degreesOfFreedom).density(x)
  }

  def toObjectArr(v: Any): Array[Any] = {
    v match {
      case arr: Array[_] => arr.map(_.asInstanceOf[Any])
      case prod: scala.Product =>
        prod.productIterator.map {
          case v: Array[_] => new GenericArrayData(v)
          case v => v
        }.toArray
    }
  }
}
