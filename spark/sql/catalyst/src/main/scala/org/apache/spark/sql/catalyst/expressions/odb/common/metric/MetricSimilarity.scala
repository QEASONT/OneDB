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


import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

import scala.math.{abs, min}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants


trait ODBSimilarity extends Serializable {
  //  def evalWithPoint(t1: Point[Any], t2: Point[Any]): Double

  def evalWithPoint(t1: Any, t2: Any): Double

  def calDist(t1: Array[Double], t2: Array[Double]): Double

  def calWordDist(s1: String, s2: String): Double
}

object ODBSimilarity {
  def getDistanceFunction(function: ODBSimilarityFunction):
  ODBSimilarity = function match {
    case ODBSimilarityFunction.EUCLID => ODBSimilarity.EUCLIDistance
    case ODBSimilarityFunction.COSINE => ODBSimilarity.COSINEDistance
    case ODBSimilarityFunction.L1 => ODBSimilarity.L1Distance
    case ODBSimilarityFunction.EDIT => ODBSimilarity.EDITDistance
    case ODBSimilarityFunction.TIME => ODBSimilarity.TIMEDistance
    case ODBSimilarityFunction.CUSTOM => ODBSimilarity.CUSTOMDistance
  }


  object EUCLIDistance extends ODBSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(ODBConfigConstants.THRESHOLD_LIMIT)
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
        ans += (t1(i) - t2(i)) * (t1(i) - t2(i))
      Math.sqrt(ans)
    }

    override def calWordDist(s1: String, s2: String): Double = {
      0
    }
  }

  object COSINEDistance extends ODBSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(ODBConfigConstants.THRESHOLD_LIMIT)

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

  object L1Distance extends ODBSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(ODBConfigConstants.THRESHOLD_LIMIT)

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

  object EDITDistance extends ODBSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(ODBConfigConstants.THRESHOLD_LIMIT)

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


      for (i <- 0 to m) dist(i)(0) = i
      for (j <- 0 to n) dist(0)(j) = j

      
      for (i <- 1 to m; j <- 1 to n) {
        val cost = if (s1(i - 1) == s2(j - 1)) 0 else 1
        dist(i)(j) = math.min(
          math.min(dist(i - 1)(j) + 1, dist(i)(j - 1) + 1),
          dist(i - 1)(j - 1) + cost
        )
      }

      dist(m)(n).toDouble
    }


  }

  object TIMEDistance extends ODBSimilarity {
    //    private final val MAX_COST = Array.fill[Double](1, 1)(ODBConfigConstants.THRESHOLD_LIMIT)
    def trans(ddd1: Float, ddd2: Float): Int = {
      val dd1 = ddd1.toInt
      val dd2 = ddd2.toInt

      val m1 = ((dd1 % 10000) / 100 + 9) % 12
      val y1 = dd1 / 10000 - m1 / 10
      val d1 = 365 * y1 + y1 / 4 - y1 / 100 + y1 / 400 + (m1 * 306 + 5) / 10 + (dd1 % 100 - 1)

      val m2 = ((dd2 % 10000) / 100 + 9) % 12
      val y2 = dd2 / 10000 - m2 / 10
      val d2 = 365 * y2 + y2 / 4 - y2 / 100 + y2 / 400 + (m2 * 306 + 5) / 10 + (dd2 % 100 - 1)

      d1 - d2
    }

    def trans2(ddd1: Float, ddd2: Float): Int = {
      val dd1 = ddd1.toInt
      val dd2 = ddd2.toInt

      val y1 = dd1 / 10000
      val m1 = (dd1 / 100) % 100
      val d1 = dd1 % 100

      val y2 = dd2 / 10000
      val m2 = (dd2 / 100) % 100
      val d2 = dd2 % 100

      ((y1 - y2) * 60 + m1 - m2) * 60 + d1 - d2
    }

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
      var tmp = 0
      // Time
      tmp += trans(t1(0).toFloat, t2(0).toFloat)
      if (t2(0) - t1(0) > 1) {
        tmp = tmp * 24 * 3600 + trans2(t1(1).toFloat, t2(1).toFloat)
      }
      abs(tmp)
    }

    override def calWordDist(s1: String, s2: String): Double = {
      0
    }
  }

  trait DistanceFunction {
    def calculateDistance(a: Any, b: Any): Double
  }

  object DynamicDistanceFunctionLoader {
    private var loadedFunction: Option[DistanceFunction] = None

    def loadDistanceFunction(hdfsPath: String): DistanceFunction = {
      loadedFunction match {
        case Some(function) => function
        case None =>
          val conf = new Configuration()
          //          val fs = FileSystem.get(conf)
          val path = new Path(hdfsPath)
          //          val inputStream = fs.open(path)

          val fs = FileSystem.get(path.toUri, new Configuration())
          val inputStream = fs.open(path)

          val code = scala.io.Source.fromInputStream(inputStream).mkString.trim
          inputStream.close()

          val toolbox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
          val tree = toolbox.parse(code)
          val compiledCode = toolbox.compile(tree)
          val instance = compiledCode().asInstanceOf[DistanceFunction]
          loadedFunction = Some(instance)
          instance
      }
    }
  }

  object CUSTOMDistance extends ODBSimilarity {
    lazy val distanceFunction: DistanceFunction = DynamicDistanceFunctionLoader.loadDistanceFunction(ODBConfigConstants.DISTANCE_FUNCTION_PATH)

    override def evalWithPoint(t1: Any, t2: Any): Double = {
      distanceFunction.calculateDistance(t1, t2)
    }

    override def calDist(t1: Array[Double], t2: Array[Double]): Double = {
      0
    }

    override def calWordDist(s1: String, s2: String): Double = {
      0
    }
  }

}