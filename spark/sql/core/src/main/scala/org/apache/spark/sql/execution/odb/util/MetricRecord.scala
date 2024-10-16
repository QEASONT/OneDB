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

package org.apache.spark.sql.execution.odb.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

import scala.math.{abs, min, sqrt}

case class MetricRecord(id: Long, metricDouble: Array[Array[Double]], metricString: Array[String], metricM: Array[Int], metricMaxDis: Array[Double]) {
  override def toString: String = {
    val res = metricM.map {
      case x@(0 | 1 | 2 | 3 | 5) => metricDouble(x).mkString(",")
      case x@4 => metricDouble(x).mkString(",")
      case x@6 => metricString(x)
    }.mkString(" ")
    res
  }
}

object MetricRecord {

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

  def distance(metricRecord1: MetricRecord, metricRecord2: MetricRecord, m: Int, metricM: Array[Int], metricMaxDis: Array[Double]): Double = {
    var dist: Double = metricM(m) match {
      case 0 | 1 | 4 =>
        // L1
        var tmp: Double = 0
        for (i <- metricRecord1.metricDouble(m).indices) {
          tmp += math.abs(metricRecord1.metricDouble(m)(i) - metricRecord2.metricDouble(m)(i))
        }
        tmp
      case 5 =>
        var tmp: Double = 0
        // Cosine
        val sum1 = metricRecord1.metricDouble(m).map(x => x * x).sum
        val sum2 = metricRecord2.metricDouble(m).map(x => x * x).sum
        val sum3 = metricRecord1.metricDouble(m).zip(metricRecord2.metricDouble(m)).map(x => x._1 * x._2).sum
        tmp = sum3 / (sqrt(sum1) * sqrt(sum2))
        Math.abs(Math.acos(tmp) * 180 / Math.PI)
      case 2 =>
        var tmp: Double = 0
        // Euclid
        for (i <- metricRecord1.metricDouble(m).indices) {
          tmp += math.pow(metricRecord1.metricDouble(m)(i) - metricRecord2.metricDouble(m)(i), 2)
        }
        sqrt(tmp)
      case 3 =>
        var tmp = 0
        // Time
        tmp += trans(metricRecord1.metricDouble(m)(0).toFloat, metricRecord2.metricDouble(m)(0).toFloat)
        if (metricRecord2.metricDouble(m)(0) - metricRecord1.metricDouble(m)(0) > 1) {
          tmp = tmp * 24 * 3600 + trans2(metricRecord1.metricDouble(m)(1).toFloat, metricRecord2.metricDouble(m)(1).toFloat)
        }
        abs(tmp)
      case 6 =>
        // Edit Distance
        //          val s1 = metricRecord1.metricString(m)
        //          val s2 = metricRecord2.metricString(m)
        //          val len1 = s1.length
        //          val len2 = s2.length
        //          val dist = Array.ofDim[Int](len1 + 1, len2 + 1)
        //
        //          // initialize matrix
        //          (0 to len1).foreach { i => dist(i)(0) = if (i > 0) dist(i - 1)(0) + s1(i - 1).toInt else 0 }
        //          (0 to len2).foreach { j => dist(0)(j) = if (j > 0) dist(0)(j - 1) + s2(j - 1).toInt else 0 }
        //
        //          // calculate edit dist
        //          (1 to m).foreach { i =>
        //            (1 to len2).foreach { j =>
        //              val replaceCost = math.abs(s1(i - 1).toInt - s2(j - 1).toInt)
        //              val insertCost = dist(i)(j - 1) + s2(j - 1).toInt
        //              val deleteCost = dist(i - 1)(j) + s1(i - 1).toInt
        //              val replaceCostTotal = dist(i - 1)(j - 1) + replaceCost
        //              dist(i)(j) = min(min(insertCost, deleteCost), replaceCostTotal)
        //            }
        //          }
        //
        //          dist(len1)(len2)
        val s1 = metricRecord1.metricString(m)
        val s2 = metricRecord2.metricString(m)
        if (s1.isEmpty) {
          s2.length
        } else if (s2.isEmpty) {
          s1.length
        } else {
          val len1 = s1.length
          val len2 = s2.length
          val distMatrix = Array.ofDim[Int](len1 + 1, len2 + 1)
          for (i <- 0 to len1) {
            distMatrix(i)(0) = i
          }
          for (j <- 0 to len2) {
            distMatrix(0)(j) = j
          }
          for (i <- 1 to len1) {
            for (j <- 1 to len2) {
              val cost = if (s1(i - 1) == s2(j - 1)) 0 else 1
              distMatrix(i)(j) = min(min(distMatrix(i - 1)(j) + 1, distMatrix(i)(j - 1) + 1), distMatrix(i - 1)(j - 1) + cost)
            }
          }
          distMatrix(len1)(len2)
        }
    }
    dist / metricMaxDis(m)
  }

  def getMetric(line: (String, Long), metricM: Array[Int], metricMaxDis: Array[Double]): MetricRecord = {
    val metricDouble = new Array[Array[Double]](metricM.length)
    val metricString = new Array[String](metricM.length)
    line._1.split(" ").zipWithIndex.foreach(x => {
      val index = x._2
      val value = x._1
      metricM(index) match {
        case 0 | 1 | 2 | 3 | 5 => metricDouble(index) = value.split(",").map(x => x.toDouble) // Double
        case 4 => metricDouble(index) = value.split(",").map(x => x.toDouble) // Actually Int Array
        case 6 => metricString(index) = value // String Array
      }
    })
    MetricRecord(line._2, metricDouble, metricString, metricM, metricMaxDis)
  }

  def metricToPoint(line: (String, Long), metricM: Array[Int], metricMaxDis: Array[Double]): MetricData = {
    val metricDouble = new Array[Array[Double]](metricM.length)
    val metricString = new Array[String](metricM.length)
    val tmp = line._1.split(" ").zipWithIndex.map(x => {
      val index = x._2
      val value = x._1
      metricM(index) match {
        case 0 | 1 | 2 | 3 | 5 => Point[Any](value.split(",").map(x => x.toDouble), index, metricM(index)) // Double
        case 4 => Point[Any](value.split(",").map(x => x.toDouble), index, metricM(index)) // Actually Int Array
        case 6 => Point[Any](value, index, metricM(index)) // String Array
      }
    })
    MetricData(tmp)
  }

  // calculate metricmeandis
  def calculateMetricMeanDis(records: RDD[MetricRecord], metricM: Array[Int], metricMaxDis: Array[Double]): Array[Double] = {
    val metricMeanDis = new Array[Double](records.first().metricDouble.length)
    //    val total = records.count()
    for (i <- metricMeanDis.indices) {
      for (j <- 0 until 1000) {
        // randomly choose two
        val record1 = records.sample(withReplacement = false, 0.1).first()
        val record2 = records.sample(withReplacement = false, 0.1).first()
        metricMeanDis(i) += distance(record1, record2, i, metricM, metricMaxDis)
      }
    }
    metricMeanDis

    //      records.sample(withReplacement = false, 0.1).collect().foreach(record => {
    //        for (j <- record.metricDouble.indices) {
    //          val sum = record.metricDouble(j).sum
    //          metricMeanDis(j) += sum
    //        }
    //      })
    //      for (j <- metricMeanDis.indices) {
    //        metricMeanDis(j) /= records.count()
    //      }
    //      metricMeanDis
  }
}