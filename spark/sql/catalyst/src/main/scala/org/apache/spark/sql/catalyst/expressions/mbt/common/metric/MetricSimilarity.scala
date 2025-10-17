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

package org.apache.spark.sql.catalyst.expressions.mbt.common.metric

import org.apache.spark.sql.catalyst.expressions.mbt.MBTSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.mbt.common.shape.Point

import scala.math.min
import scala.reflect.ClassTag

object PointDistance {
  //  def eval(v: Point[Any]): Double = {
  //    math.sqrt(v.coord.map(x => x * x).sum)
  //  }
}

trait MBTSimilarity extends Serializable {
  //  def evalWithPoint(t1: Point[Any], t2: Point[Any]): Double

  def evalWithPoint(t1: Any, t2: Any): Double

  def calDist(t1: Array[Double], t2: Array[Double]): Double

  def calWordDist(s1: String, s2: String): Double
}

object MBTSimilarity {
  def getDistanceFunction(function: MBTSimilarityFunction):
  MBTSimilarity = function match {
    case MBTSimilarityFunction.EUCLID => MBTSimilarity.EUCLIDistance
    case MBTSimilarityFunction.COSINE => MBTSimilarity.COSINEDistance
    case MBTSimilarityFunction.L1 => MBTSimilarity.L1Distance
    case MBTSimilarityFunction.EDIT => MBTSimilarity.EDITDistance
  }


  object EUCLIDistance extends MBTSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(MBTConfigConstants.THRESHOLD_LIMIT)
    override def evalWithPoint(t1: Any, t2: Any): Double = {
      t1 match {
        case doubles: Array[Double] =>
          t2 match {
            case doubles2: Array[Double] =>
              calDist(doubles, doubles2)
            case point: Point[Any] =>
              calDist(doubles, point.coord.asInstanceOf[Array[Double]])
            case _ =>
              0
          }
        case _ => 0
      }
    }

    //    override def evalWithPoint(t1: Point[Any], t2: Point[Any]): Double = {
    //      val t1Array = t1.coord.asInstanceOf[Array[Double]]
    //      val t2Array = t2.coord.asInstanceOf[Array[Double]]
    //      calDist(t1Array, t2Array)
    //    }

    //    override def evalWithPoint(t1: Any, t2: Point[Any]): Double = {
    //      val t1Array = t1.asInstanceOf[Array[Double]]
    //      val t2Array = t2.coord.asInstanceOf[Array[Double]]
    //      calDist(t1Array, t2Array)
    //    }
    //
    //    override def evalWithPoint(t1: Point[Any], t2: Any): Double = {
    //      val t1Array = t1.coord.asInstanceOf[Array[Double]]
    //      val t2Array = t2.asInstanceOf[Array[Double]]
    //      calDist(t1Array, t2Array)
    //    }

    override def calDist(t1: Array[Double], t2: Array[Double]): Double = {
      require(t1.length == t2.length)
      var ans = 0.0
      for (i <- t1.indices)
        ans += (t1(i) - t2(i)) * (t1(i) - t2(i))
      Math.sqrt(ans)
    }

    override def calWordDist(s1: String, s2: String): Double = {
      0
    }
  }

  object COSINEDistance extends MBTSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(MBTConfigConstants.THRESHOLD_LIMIT)

    override def evalWithPoint(t1: Any, t2: Any): Double = {
      t1 match {
        case doubles: Array[Double] =>
          t2 match {
            case doubles2: Array[Double] =>
              calDist(doubles, doubles2)
            case point: Point[Any] =>
              calDist(doubles, point.coord.asInstanceOf[Array[Double]])
            case _ =>
              0
          }
        case _ => 0
      }
    }

    override def calDist(t1: Array[Double], t2: Array[Double]): Double = {
      require(t1.length == t2.length)
      val a = Math.sqrt(t1.map(x => x * x).sum)
      val b = Math.sqrt(t2.map(x => x * x).sum)
      val c = t1.zip(t2).map(x => x._1 * x._2).sum
      c / (a * b)
    }

    override def calWordDist(s1: String, s2: String): Double = {
      0
    }

  }

  object L1Distance extends MBTSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(MBTConfigConstants.THRESHOLD_LIMIT)

    override def evalWithPoint(t1: Any, t2: Any): Double = {
      t1 match {
        case doubles: Array[Double] =>
          t2 match {
            case doubles2: Array[Double] =>
              calDist(doubles, doubles2)
            case point: Point[Any] =>
              calDist(doubles, point.coord.asInstanceOf[Array[Double]])
            case _ =>
              0
          }
        case _ => 0
      }
    }

    override def calDist(t1: Array[Double], t2: Array[Double]): Double = {
      require(t1.length == t2.length)
      var ans = 0.0
      for (i <- t1.indices)
        ans += (t1(i) - t2(i)).abs
      ans
    }

    override def calWordDist(s1: String, s2: String): Double = {
      0
    }


  }

  object EDITDistance extends MBTSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(MBTConfigConstants.THRESHOLD_LIMIT)

    override def evalWithPoint(t1: Any, t2: Any): Double = {
      t1 match {
        case s1: String =>
          t2 match {
            case s2: String =>
              calWordDist(s1, s2)
            case point: Point[Any] =>
              calWordDist(s1, point.coord.asInstanceOf[String])
            case _ =>
              0
          }
        case _ => 0
      }
    }

    override def calDist(t1: Array[Double], t2: Array[Double]): Double = {
      0
    }

    override def calWordDist(s1: String, s2: String): Double = {
      val m = s1.length
      val n = s2.length
      val dist = Array.ofDim[Int](m + 1, n + 1)

      // initialize matrix
      (0 to m).foreach { i => dist(i)(0) = if (i > 0) dist(i - 1)(0) + s1(i - 1).toInt else 0 }
      (0 to n).foreach { j => dist(0)(j) = if (j > 0) dist(0)(j - 1) + s2(j - 1).toInt else 0 }

      // calculate edit dist
      (1 to m).foreach { i =>
        (1 to n).foreach { j =>
          val replaceCost = math.abs(s1(i - 1).toInt - s2(j - 1).toInt)
          val insertCost = dist(i)(j - 1) + s2(j - 1).toInt
          val deleteCost = dist(i - 1)(j) + s1(i - 1).toInt
          val replaceCostTotal = dist(i - 1)(j - 1) + replaceCost
          dist(i)(j) = min(min(insertCost, deleteCost), replaceCostTotal)

          // swap
          //      if (i > 1 && j > 1 && s1(i - 1) == s2(j - 2) && s1(i - 2) == s2(j - 1)) {
          //        val swapCost = dist(i - 2)(j - 2) + math.abs(s1(i - 1).toInt - s1(i - 2).toInt) + math.abs(s2(j - 1).toInt - s2(j - 2).toInt)
          //        dist(i)(j) = min(dist(i)(j), swapCost)
          //      }
        }
      }

      dist(m)(n)
    }


    //  def evalWithMetric(p1: Point, p2: Point): Double = {
    //    var ans = 0.0
    //    for (i <- p1.coord.indices)
    //      ans += (p1.coord(i) - p2.coord(i)) * (p1.coord(i) - p2.coord(i))
    //    Math.sqrt(ans)
    //  }

  }

}