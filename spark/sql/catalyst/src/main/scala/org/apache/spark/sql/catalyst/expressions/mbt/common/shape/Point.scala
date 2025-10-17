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
package org.apache.spark.sql.catalyst.expressions.mbt.common.shape

import org.apache.spark.sql.catalyst.expressions.mbt.{MBTSimilarityExpression, MBTSimilarityFunction}
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants
import org.apache.spark.sql.catalyst.expressions.mbt.common.metric.MBTSimilarity

import scala.math.min
import scala.math.max

case class Point[+T](coord: T) extends Shape {

  def this() = this(null.asInstanceOf[T])

  //  @transient
  //  var globalPivot: Point = _
  //  @transient
  //  var pivotDistance: Double = _
  //  @transient
  //  var globalIndex: Int = _

  override def minDist(other: Any, distanceFunction: MBTSimilarityFunction): Double = {
    MBTSimilarity.getDistanceFunction(distanceFunction).evalWithPoint(coord, other)
  }

  def ~=(x: Double, y: Double, precision: Double): Boolean = {
    if ((x - y).abs < precision) true else false
  }

  def ==(other: Point[Any]): Boolean =
    other.coord match {
      case p: Array[Double] =>
        if (p.length !=
          coord.asInstanceOf[Array[Double]].length) {
          false
        } else {
          for (i <- coord.asInstanceOf[Array[Double]].indices)
            if (! ~=(coord.asInstanceOf[Array[Double]](i),
              p(i), 0.000001)) {
              return false
            }
          true
        }
      case p: String =>
        if (p.length != coord.asInstanceOf[String].length) {
          false
        } else {
          for (i <- coord.asInstanceOf[String].indices)
            if (coord.asInstanceOf[String](i) != p(i)) {
              return false
            }
          true
        }
      case _ => false
    }

  override def toString: String = {
    def doubleToString(coord: Array[Double]): String = {
      var s = "POINT("
      s += coord(0).toString
      for (i <- 1 until coord.length) s += "," + coord(i)
      s + ")"
    }

    def stringToString(coord: String): String = {
      var s = "POINT("
      s += coord
      s + ")"
    }

    coord match {
      case p: Array[Double] => doubleToString(p)
      case p: String => stringToString(p)
      case _ => null
    }
  }
}