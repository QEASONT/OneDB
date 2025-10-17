/*
 *  Copyright 2023 by DIMS Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.spark.sql.catalyst.expressions.odb.common.metric

import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.slf4j.{Logger, LoggerFactory}
import hep.aida.bin.StaticBin1D

import scala.util.Random

import scala.collection.immutable.HashSet
import cern.colt.list.LongArrayList
import scala.util.control.Breaks.break

case class MetricData(points: Array[Point[Any]]) {
  require(points.nonEmpty)
  val id: Long = points(0).id

  def minDist(other: MetricData): Double = {
    points.zipWithIndex.map {
      case (p, i) => p.minDist(other.points(i))
    }.sum
  }

  def minDist(other: MetricData, nonZeroMetric: Array[(Double, Int)]): Double = {
    nonZeroMetric.map {
      case (weight, index) => weight * points(index).minDist(other.points(index))
    }.sum
  }

  def minDist(other: Point[Any]): Double = {
    val metricIndex = other.metricIndex
    points(metricIndex).minDist(other)
  }

  def originDist(other: MetricData): Array[Double] = {
    points.zipWithIndex.map {
      case (p, i) => p.minDist(other.points(i))
    }
  }

  /**
   * Alpha is the number of positive or negative STDS that we will add.
   */

  override def toString: String = {
    s"Metric(points = ${points.mkString(",")}"
  }


}