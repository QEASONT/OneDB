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

package org.apache.spark.sql.catalyst.expressions.odb.common.shape

case class SimpleRange(dimensionIndex: Int, low: Double, high: Double) extends Shape {
  require(low <= high, s"$low, $high")

  override def minDist(other: Any, queryM: Array[(Double, Int)] = null): Double = {
    other match {
      case p: Point[Any] =>
        if (p.coord.asInstanceOf[Array[Double]](dimensionIndex) < low) {
          low - p.coord.asInstanceOf[Array[Double]](dimensionIndex)
        } else if (p.coord.asInstanceOf[Array[Double]](dimensionIndex) > high) {
          p.coord.asInstanceOf[Array[Double]](dimensionIndex) - high
        } else {
          math.min(p.coord.asInstanceOf[Array[Double]](dimensionIndex) - low, high - p.coord.asInstanceOf[Array[Double]](dimensionIndex))
        }
      case mbr: Rectangle =>
        if (mbr.high.coord.asInstanceOf[Array[Double]](dimensionIndex) < low) {
          low - mbr.high.coord.asInstanceOf[Array[Double]](dimensionIndex)
        } else if (high < mbr.low.coord.asInstanceOf[Array[Double]](dimensionIndex)) {
          mbr.low.coord.asInstanceOf[Array[Double]](dimensionIndex) - high
        } else {
          0.0
        }
    }
  }

  def intersects(other: Shape): Boolean = {
    other match {
      case p: Point[Any] => low <= p.coord.asInstanceOf[Array[Double]](dimensionIndex) && p.coord.asInstanceOf[Array[Double]](dimensionIndex) <= high
      case mbr: Rectangle => mbr.high.coord.asInstanceOf[Array[Double]](dimensionIndex) >= low &&
        high >= mbr.low.coord.asInstanceOf[Array[Double]](dimensionIndex)
    }
  }

  override def dist(other: Any, queryM: Array[(Double, Int)]): Double = -1
}
