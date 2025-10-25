/*
 *  Copyright 2025 by ODB Project
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
package org.apache.spark.sql.execution.odb.partition.local

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.dita.common.DITAConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.{Point, Rectangle}
import org.apache.spark.sql.execution.odb.index.hnsw.RefHnsw
import org.apache.spark.sql.execution.odb.index.local.RTree
import org.apache.spark.util.SizeEstimator
import com.qt.kahip.KaHIPWrapper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.odb.index.MVPTree.MVPTree
import org.apache.spark.sql.execution.odb.index.entity.{MVPDP, MVPError}
import org.apache.spark.sql.execution.odb.index.local.InvertedIndex

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable
import scala.math.{log, max, min, pow}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class Bounds(min: Array[Double], max: Array[Double])

case class LocalODBPartitioner(
                                RtreeMaxEntriesPerNode: Int,
                                metricData: Array[MetricData]) extends Logging {
  val metricM = getMetricM()
  //  val BPlusTree = buildBPlusTree()
  //  val fineGrainedPoint = cutMetricDataToPoint()
  val rTreeForest = new Array[RTree](metricM.length)
  val mvpTreeForest = new Array[MVPTree](metricM.length)
  val invertIndexForest = new Array[InvertedIndex](metricM.length)
  val mbrBoundsForest = new Array[Array[(Rectangle, Int)]](metricM.length)

  val distanceOrd = Ordering.Double
  var dataShuffled: Array[Array[Array[Point[Any]]]] = _
  val numOfPartition: Array[Int] = new Array[Int](metricM.length)
  val modalPoints: Array[Array[(Long, Point[Any])]] = metricM.indices.map { metricIndex =>
    metricData.map(x => (x.id, x.points(metricIndex)))
  }.toArray

  val bf = 2
  val p = 20
  val k = 25


  def deleteMetricData(metricData: MetricData): List[(MetricData, Double)] = {


    val res = metricM.zipWithIndex.map {
      case (metricValue, metricIndex) =>
        metricValue match {
          case 0 | 1 | 4 | 5 =>
            val targetPoint = metricData.points(metricIndex)
            val deletedEntry = rTreeForest(metricIndex).delete((targetPoint.id.toInt, targetPoint))
            (metricData, if (deletedEntry != null) 0.0 else 1.0)
          case 2 | 3 =>
            val mvpdp = new MVPDP(0, metricData.points(metricIndex))
            mvpTreeForest(metricIndex).delete(mvpdp)
            (metricData, 0.0)
          case 6 =>
            val stringPoint = metricData
            invertIndexForest(metricIndex).delete(stringPoint.id.toInt)
            (metricData, 0.0)
        }
    }
    res.toList
  }

  def insertMetricData(metricData: MetricData): List[(MetricData, Double)] = {


    val res = metricM.zipWithIndex.map {
      case (metricValue, metricIndex) =>
        metricValue match {
          case 0 | 1 | 4 | 5 =>
            // Rtree
            val newPoint = metricData.points(metricIndex)
            val newEntry = (newPoint.id, newPoint)
            rTreeForest(metricIndex).insert(newEntry, RtreeMaxEntriesPerNode)
            (metricData, 0.0)
          case 2 | 3 =>
            val mvpdp = new MVPDP(0, metricData.points(metricIndex))
            mvpTreeForest(metricIndex).mvpAdd(List(mvpdp).asJava, 1)
            (metricData, 0.0)
          case 6 =>
            val stringPoint = metricData.points(metricIndex)
            invertIndexForest(metricIndex).insert(stringPoint)
            (metricData, 0.0)
        }
    }
    res.toList
  }


  //  var dataShuffled: Array[Array[Point[Any]]] = _
  private def getMetricM(): Array[Int] = {
    //    val aa = metricData.length
    metricData.take(1).head.points.map(x => x.metricValue)
  }


  private def buildAdaptiveTree(): Unit = {
    metricM.zipWithIndex.par.foreach {
      case (metricValue, metricIndex) =>
        metricValue match {
          case 0 | 1 | 4 | 5 =>

            rTreeForest(metricIndex) =
              RTree(modalPoints(metricIndex), RtreeMaxEntriesPerNode)

          case 2 | 3 =>
            mvpTreeForest(metricIndex) = new MVPTree(bf, p, k, 0, 0, 0, 0, null)
            val mvpdpArray = modalPoints(metricIndex).map { x =>
              new MVPDP(x._1.toInt, x._2)
            }
            mvpTreeForest(metricIndex).mvpAdd(new java.util.ArrayList(mvpdpArray.toList.asJava), metricData.length)
          case 6 =>
            val stringPointMap = modalPoints(metricIndex).toMap
            invertIndexForest(metricIndex) = new InvertedIndex(stringPointMap)


        }

    }
  }

  //  def calMbrBounds(metricIndex: Int, metricValue: Int): Array[(Rectangle, Int)] = {
  //    val (dataBounds, totalCount) = getBoundsAndCount(metricIndex, metricValue)
  //    val data = metricData
  //    val RtreeDimension = metricData.take(1).head.points(metricIndex).coord.asInstanceOf[Array[Double]].length
  //    val mbrs = if (RtreeNumOfPartition > 1) {
  //      val dimensionCount = new Array[Int](RtreeDimension)
  //      var remaining = RtreeNumOfPartition.toDouble
  //      for (i <- 0 until RtreeDimension) {
  //        dimensionCount(i) = Math.ceil(Math.pow(remaining, 1.0 / (RtreeDimension - i))).toInt
  //        remaining /= dimensionCount(i)
  //      }
  //
  //      val currentBounds = Bounds(new Array[Double](RtreeDimension), new Array[Double](RtreeDimension))
  //      recursiveGroupPoint(dimensionCount, dataBounds, data, metricIndex, metricValue, currentBounds, 0, RtreeDimension - 1)
  //
  //    } else {
  //      if (dataBounds == null) {
  //        val min = new Array[Double](RtreeDimension).map(_ => Double.MaxValue)
  //        val max = new Array[Double](RtreeDimension).map(_ => Double.MinValue)
  //        Array(Rectangle(Point[Any](min, metricIndex, metricValue), Point[Any](max, metricIndex, metricValue)))
  //      } else {
  //        Array(Rectangle(Point[Any](dataBounds.min, metricIndex, metricValue),
  //          Point[Any](dataBounds.max, metricIndex, metricValue)))
  //      }
  //    }
  //
  //    mbrs.zipWithIndex
  //  }

  //  def insertMetricData(metricData: Array[MetricData]): Unit = {
  //
  //  }

  def getBoundsAndCount(metricIndex: Int, metricValue: Int): (Bounds, Long) = {
    metricData.aggregate[(Bounds, Long)]((null, 0))((bound, data) => {
      val new_bound = if (bound._1 == null) {
        Bounds(data.points(metricIndex).coord.asInstanceOf[Array[Double]], data.points(metricIndex).coord.asInstanceOf[Array[Double]])
      } else {
        Bounds(bound._1.min.zip(data.points(metricIndex).coord.asInstanceOf[Array[Double]]).map(x => Math.min(x._1, x._2)),
          bound._1.max.zip(data.points(metricIndex).coord.asInstanceOf[Array[Double]]).map(x => Math.max(x._1, x._2)))
      }
      (new_bound, bound._2 + SizeEstimator.estimate(data))
    }, (left, right) => {
      val new_bound = {
        if (left._1 == null) {
          right._1
        } else if (right._1 == null) {
          left._1
        } else {
          Bounds(left._1.min.zip(right._1.min).map(x => Math.min(x._1, x._2)),
            left._1.max.zip(right._1.max).map(x => Math.max(x._1, x._2)))
        }
      }
      (new_bound, left._2 + right._2)
    })
  }

  def recursiveGroupPoint(dimensionCount: Array[Int], dataBounds: Bounds,
                          entries: Array[MetricData], metricIndex: Int, metricValue: Int, currentBounds: Bounds,
                          currentDimension: Int, untilDimension: Int): Array[Rectangle] = {
    var ans = mutable.ArrayBuffer[Rectangle]()
    if (entries.isEmpty) {
      return ans.toArray
    }

    val len = entries.length.toDouble
    val grouped = entries.sortWith(_.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
        < _.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension))
      .grouped(Math.ceil(len / dimensionCount(currentDimension)).toInt).toArray
    if (currentDimension < untilDimension) {
      for (i <- grouped.indices) {
        if (i == 0 && i == grouped.length - 1) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else if (i == 0) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
        } else if (i == grouped.length - 1) {
          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else {
          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
        }
        ans ++= recursiveGroupPoint(dimensionCount, dataBounds, grouped(i), metricIndex, metricValue,
          currentBounds, currentDimension + 1, untilDimension)
      }
      ans.toArray
    } else {
      for (i <- grouped.indices) {
        if (i == 0 && i == grouped.length - 1) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else if (i == 0) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
        } else if (i == grouped.length - 1) {
          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else {
          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
        }
        ans += Rectangle(Point[Any](currentBounds.min.clone(), metricIndex, metricValue),
          Point[Any](currentBounds.max.clone(), metricIndex, metricValue))
      }
      ans.toArray
    }
  }

  def getResultWithKnn(query: MetricData, k: Int, nonZeroQueryM: Array[(Double, Int)]): Double = {
    0
  }

  def getResultWithThreshold(query: MetricData, threshold: Array[Double], nonZeroQueryM: Array[(Double, Int)]): Array[Long] = {
    val start = System.currentTimeMillis()
    val res = nonZeroQueryM.flatMap {
      case (queryMetric, queryIndex) =>
        val queryPoint = query.points(queryIndex)
        val metricValue = queryPoint.metricValue
        val metricIndex = queryPoint.metricIndex
        metricValue match {
          case 0 | 1 | 4 | 5 =>
            // Rtree
            val res = rTreeForest(metricIndex).circleRange(queryPoint, threshold(queryIndex))
            res.map(x => x._1.asInstanceOf[Point[Any]].id)

          case 2 | 3 =>
            // MVP
            val error = MVPError.MVP_SUCCESS
            val mvpdp = new MVPDP(0, queryPoint)
            mvpTreeForest(metricIndex).mvpRetrieve(mvpdp, threshold(queryIndex), error).map(_.id)
          case 6 =>
            // Inverted Index
            val queryStr = queryPoint.coord.asInstanceOf[String]
            val range = threshold(queryIndex)
            val rangeSearch = invertIndexForest(metricIndex).rangeSearch(queryStr, range)
            rangeSearch.map(x => x._1)

        }
    }
    val end = System.currentTimeMillis()
    logWarning(s"Local ODB Search Time: ${end - start} ms")
    res
  }


}

object LocalODBPartitioner {


  private val maxEntriesPerNode = ODBConfigConstants.RTREE_LOCAL_MAX_ENTRIES_PER_NODE
  //  private val numOfPartition = ODBConfigConstants.RTREE_LOCAL_NUM_PARTITIONS


  def partition(metricData: Array[MetricData]):
  (Array[Array[MetricData]], LocalODBPartitioner) = {

    val partitioner = new LocalODBPartitioner(maxEntriesPerNode, metricData)
    partitioner.buildAdaptiveTree()

    (null, partitioner)
  }

}
