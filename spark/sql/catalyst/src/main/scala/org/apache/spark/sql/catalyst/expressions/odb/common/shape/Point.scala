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

import org.apache.spark.sql.catalyst.expressions.odb.{ODBSimilarityExpression, ODBSimilarityFunction}
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.ODBSimilarity

import scala.math.min
import scala.math.max

case class Point[+T](coord: T, metricIndex: Int, metricValue: Int, metricMaxDis: Double = 0, id: Long = -1) extends Shape {

  def this() = this(coord = null.asInstanceOf[T]
    , metricIndex = -1, metricValue = -1, metricMaxDis = -1)

  def getCoordinate(axis: Int): Double = {
    coord match {
      case p: Array[Double] => p(axis)
      case _ => 0
    }
  }

  def makeCopy: Point[Any] =
    new Point[Any](coord, metricIndex, metricValue, metricMaxDis, id)

  override def minDist(other: Any, queryM: Array[(Double, Int)] = null): Double = {
    var normalize: Double = metricMaxDis
    if (metricMaxDis == 0) {
      normalize = other match {
        case p: Point[Any] => p.metricMaxDis
        case rectangle: Rectangle => rectangle.minDist(this, queryM)
      }
    }

    metricValue match {
      case 0 | 1 | 4 =>
        // L1
        ODBSimilarity.getDistanceFunction(ODBSimilarityFunction.L1).evalWithPoint(coord, other) / normalize
      case 5 =>
        // Cosine
        ODBSimilarity.getDistanceFunction(ODBSimilarityFunction.COSINE).evalWithPoint(coord, other) / normalize
      case 2 =>
        // Euclid
        ODBSimilarity.getDistanceFunction(ODBSimilarityFunction.EUCLID).evalWithPoint(coord, other) / normalize
      case 3 =>
        // Time
        ODBSimilarity.getDistanceFunction(ODBSimilarityFunction.TIME).evalWithPoint(coord, other) / normalize
      case 6 =>
        // Edit
        ODBSimilarity.getDistanceFunction(ODBSimilarityFunction.EDIT).evalWithPoint(coord, other) / normalize
      case 99 =>
        // Custom
        ODBSimilarity.getDistanceFunction(ODBSimilarityFunction.CUSTOM).evalWithPoint(coord, other) / normalize

      case _ => 0
    }
  }

  def -(other: Point[Any]): Point[Any] = Point[Any](coord.asInstanceOf[Array[Double]].zip(other.coord.asInstanceOf[Array[Double]]).map(x => x._1 - x._2), other.metricIndex, other.metricValue)

  def ~=(x: Double, y: Double, precision: Double): Boolean = {
    if ((x - y).abs < precision) true else false
  }

  def ==(other: Point[Any]): Boolean = {
    if (metricIndex != other.metricIndex) {
      false
    } else {
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
    }
  }

  override def intersects(other: Shape): Boolean = true

  override def toString: String = {
    def doubleToString(coord: Array[Double]): String = {
      coord.map(x => x.toString).mkString(",")
    }

    def stringToString(coord: String): String = {
      if (coord.startsWith("'") && coord.endsWith("'")) {
        coord
      } else {
        "'" + coord + "'"
      }
    }

    coord match {
      case p: Array[Double] => doubleToString(p)
      case p: String => stringToString(p)
      case _ => null
    }
  }

  override def dist(other: Any, queryM: Array[(Double, Int)]): Double = {
    other match {
      //      case p: Point[Any] => distPoint(p, queryM)
      case mbr: Rectangle =>
        metricValue match {
          case 0 | 1 | 4 | 5 =>
            // L1
            mbr.distPoint(this, queryM)
          case 2 =>
            // Euclid
            mbr.minDistPoint(this, queryM)
        }
      case r: SimpleRange => r.minDist(this)
      case p: Point[Any] => p.minDist(this, queryM)
      case _ =>
        // raise error
        val aa = -999
        aa

    }
  }

}