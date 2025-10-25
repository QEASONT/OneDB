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

package org.apache.spark.sql.execution.odb.partition.global

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.{Point, Rectangle}
import org.apache.spark.sql.execution.odb.index.global.MTree
import org.apache.spark.sql.execution.odb.index.global.Entry
import org.apache.spark.sql.execution.odb.index.local.RStarTree
import org.apache.spark.sql.execution.odb.partition.global.GlobalODBPartitioner.partition
import org.apache.spark.sql.execution.odb.partition.local.LocalODBPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.apache.spark.util.random.SamplingUtils

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.hashing.MurmurHash3

case class Bounds(min: Array[Double], max: Array[Double])

case class GlobalODBPartitioner(
                                 data: RDD[MetricData],
                                 maxEntriesPerNode: Int) extends Partitioner {


  private lazy val firstRow: MetricData = data.first()

  private val conf = data.sparkContext.getConf

  private val numClusters = conf.getInt("spark.odb.globalIndexedPivotCount", ODBConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT)
  private val sampleSize = conf.getLong("spark.odb.sampleSize", ODBConfigConstants.SAMPLE_SIZE)
  val metricM: Array[Int] = firstRow.points.map(_.metricValue)

  val pivots: Array[MetricData] = data.takeSample(withReplacement = false, num = 1)


  val sampleData: Array[MetricData] = sampleFixedSize(data, sampleSize.toInt, 20240926L)

  val featureVec = sampleData.map(
    x => Point[Any](
      x.originDist(pivots.head), -1, 2))
  val mbrBounds = calMbrBounds()
  val rrStarTree = buildRStarTree()

  //  val pivotMaxDist = calPivotMaxDist()
  //  val globalSamplePartitioner = LocalODBPartitioner.partition(sampleData)
  override def numPartitions: Int = {
    mbrBounds.length
  }

  private def getMetricM(): Array[Int] = {
    data.take(1).head.points.map(x => x.metricValue)
  }

  def buildRStarTree(): RStarTree = {

    RStarTree(mbrBounds.map(x => (x._1, x._2, 1)), maxEntriesPerNode)
  }

  def calMbrBounds(): Array[(Rectangle, Int)] = {
    val dataBounds = getBoundsAndCount()
    val data = featureVec
    val RtreeDimension = featureVec.take(1).head.coord.asInstanceOf[Array[Double]].length
    val mbrs = if (numClusters / 2 > 1) {
      val dimensionCount = new Array[Int](RtreeDimension)
      var remaining = (numClusters / 2).toDouble
      for (i <- 0 until RtreeDimension) {
        dimensionCount(i) = Math.ceil(Math.pow(remaining, 1.0 / (RtreeDimension - i))).toInt
        remaining /= dimensionCount(i)
      }

      val currentBounds = Bounds(new Array[Double](RtreeDimension), new Array[Double](RtreeDimension))
      recursiveGroupPoint(dimensionCount, dataBounds, data, currentBounds, 0, RtreeDimension - 1)

    } else {
      if (dataBounds == null) {
        val min = new Array[Double](RtreeDimension).map(_ => Double.MaxValue)
        val max = new Array[Double](RtreeDimension).map(_ => Double.MinValue)
        Array(Rectangle(Point[Any](min, -1, 2), Point[Any](max, -1, 2)))
      } else {
        Array(Rectangle(Point[Any](dataBounds.min, -1, 2),
          Point[Any](dataBounds.max, -1, 2)))
      }
    }

    mbrs.zipWithIndex
  }

  def getBoundsAndCount(): (Bounds) = {
    featureVec.aggregate[Bounds](null)(
      (bound, data) => {
        if (bound == null) {
          Bounds(data.coord.asInstanceOf[Array[Double]], data.coord.asInstanceOf[Array[Double]])
        } else {
          Bounds(
            bound.min.zip(data.coord.asInstanceOf[Array[Double]]).map(x => Math.min(x._1, x._2)),
            bound.max.zip(data.coord.asInstanceOf[Array[Double]]).map(x => Math.max(x._1, x._2))
          )
        }
      },
      (left, right) => {
        if (left == null) {
          right
        } else if (right == null) {
          left
        } else {
          Bounds(
            left.min.zip(right.min).map(x => Math.min(x._1, x._2)),
            left.max.zip(right.max).map(x => Math.max(x._1, x._2))
          )
        }
      }
    )
  }

  def recursiveGroupPoint(dimensionCount: Array[Int], dataBounds: Bounds,
                          entries: Array[Point[Any]], currentBounds: Bounds,
                          currentDimension: Int, untilDimension: Int): Array[Rectangle] = {
    var ans = ArrayBuffer[Rectangle]()
    if (entries.isEmpty) {
      return ans.toArray
    }

    val len = entries.length.toDouble
    val grouped = entries.sortWith(_.coord.asInstanceOf[Array[Double]](currentDimension)
        < _.coord.asInstanceOf[Array[Double]](currentDimension))
      .grouped(Math.ceil(len / dimensionCount(currentDimension)).toInt).toArray
    if (currentDimension < untilDimension) {
      for (i <- grouped.indices) {
        if (i == 0 && i == grouped.length - 1) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else if (i == 0) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord.asInstanceOf[Array[Double]](currentDimension)
        } else if (i == grouped.length - 1) {
          currentBounds.min(currentDimension) = grouped(i).head.coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else {
          currentBounds.min(currentDimension) = grouped(i).head.coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord.asInstanceOf[Array[Double]](currentDimension)
        }
        ans ++= recursiveGroupPoint(dimensionCount, dataBounds, grouped(i),
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
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord.asInstanceOf[Array[Double]](currentDimension)
        } else if (i == grouped.length - 1) {
          currentBounds.min(currentDimension) = grouped(i).head.coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else {
          currentBounds.min(currentDimension) = grouped(i).head.coord.asInstanceOf[Array[Double]](currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord.asInstanceOf[Array[Double]](currentDimension)
        }
        ans += Rectangle(Point[Any](currentBounds.min.clone(), -1, 2),
          Point[Any](currentBounds.max.clone(), -1, 2))
      }
      ans.toArray
    }
  }

  def generateCombinations(input: Array[(Int, List[Int])], m: Int): Array[Array[Int]] = {
    val inputMap = input.toMap
    val valueRanges = Array.tabulate(m + 1) { i =>
      inputMap.getOrElse(i, (0 to m).toList)
    }

    def recurse(index: Int, currentCombination: List[Int]): List[List[Int]] = {
      if (index > m) {
        List(currentCombination)
      } else {
        valueRanges(index).flatMap { value =>
          recurse(index + 1, currentCombination :+ value)
        }
      }
    }

    recurse(0, List()).map(_.toArray).toArray
  }

  def getKnnEstimatedThreshold(query: MetricData, k: Int, nonZeroQueryM: Array[(Double, Int)]): Double = {
    // calculate the estimated threshold for k-NN search
    val distances: Array[Double] = sampleData.map { data =>
      query.minDist(data, nonZeroQueryM)
    }

    val sorted = distances.sorted
    if (sorted.length < k) sorted.lastOption.getOrElse(1e10) else sorted(k - 1)

  }

  def getKnnSamplePartitions(query: MetricData, queryM: Array[(Double, Int)]): List[Int] = {
    if (rrStarTree == null) {
      List.empty
    } else {
      val k = Point[Any](query.originDist(pivots.head), -2, 2)
      //      val k = key.asInstanceOf[Point[Any]]
      val partitions = rrStarTree.circleRange(k, 0, queryM)
      val partitionNum = partitions((k.toString.hashCode % partitions.length +
        partitions.length) % partitions.length)._2
      List(partitionNum)
    }
  }

  def getPartitionsWithThreshold(query: MetricData, threshold: Double, queryM: Array[(Double, Int)]): List[Int] = {
    // QT: RR*Tree range transform?
    if (rrStarTree == null) {
      List.empty
    } else {
      val k = Point[Any](query.originDist(pivots.head), -2, 2)
      //      val k = key.asInstanceOf[Point[Any]]
      rrStarTree.circleRange(k, threshold, queryM).map(_._2)
    }
  }


  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[MetricData]
    val featurePoint = Point[Any](k.originDist(pivots.head), -1, 2)
    val partitions = rrStarTree.circleRange(featurePoint, 0.0)
    if (partitions.isEmpty) {
      0
    } else {

      val coords = featurePoint.coord.asInstanceOf[Array[Double]]
      val h = MurmurHash3.arrayHash(coords.map(java.lang.Double.hashCode))
      val idx = Math.floorMod(h, partitions.length)
      partitions(idx)._2
    }
  }


  def sampleFixedSize(
                       rdd: RDD[MetricData],
                       n: Int,
                       seed: Long,
                       withReplacement: Boolean = false
                     ): Array[MetricData] = {
    val total = rdd.count()
    if (total <= n) {
      rdd.collect()
    } else {
      val fraction = SamplingUtils.computeFractionForSampleSize(
        n, total, withReplacement
      )
      val arr = rdd.sample(withReplacement, fraction, seed).take(n)
      if (arr.length < n) {

        val missing = n - arr.length
        arr ++ rdd.takeSample(withReplacement = false, missing, seed + 1)
      } else {
        arr
      }
    }
  }

  private def farthestFirstTraversal(data: RDD[MetricData], k: Int): Array[MetricData] = {
    var centers = List.empty[MetricData]

    // Select the first center at random
    centers = data.takeSample(withReplacement = false, num = 1).toList

    // Broadcast the centers to all worker nodes
    //    var centersBroadcast = data.sparkContext.broadcast(centers)

    // Loop until we have found all the centers
    while (centers.length < k) {
      // Find the farthest point from the nearest center for each point
      val farthestPoint = data.map(point => (point, centers.minBy(center => center.minDist(point))))
        .reduce((a, b) => if (a._1.minDist(a._2) > b._1.minDist(b._2)) a else b)
        ._1

      // Add the farthest point to the centers list
      centers = farthestPoint :: centers

      // Update the broadcast variable with the new centers list
      //      centersBroadcast.unpersist()
      //      centersBroadcast = data.sparkContext.broadcast(centers)
    }

    // Convert the centers list to an array and return it
    centers.reverse.toArray
  }

}

object GlobalODBPartitioner {


  private val maxEntriesPerNode = ODBConfigConstants.RTREE_GLOBAL_MAX_ENTRIES_PER_NODE

  def partition(dataRDD: RDD[MetricData]):
  (RDD[MetricData], GlobalODBPartitioner) = {
    //    val totalCluster = ODBConfigConstants.M_TREE_ORDER
    // TODO:QT collect?


    val partitioner = new GlobalODBPartitioner(dataRDD, maxEntriesPerNode)
    println(s"GlobalODBPartitioner numPartitions = ${partitioner.numPartitions}, dataRDD.getNumPartitions = ${dataRDD.getNumPartitions}")

    //    val numOfPartitions = dataRDD.getNumPartitions
    val pairedDataRDD = dataRDD.map(x => {
      (x, None)
    })

    val shuffled = new ShuffledRDD[MetricData, Any, Any](pairedDataRDD, partitioner)


    (shuffled.map(_._1), partitioner)
  }


}
