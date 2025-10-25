///*
// *  Copyright 2025 by ODB Project
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// */
//package org.apache.spark.sql.execution.odb.partition.global
//
//import org.apache.spark.Partitioner
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.expressions.dita.common.DITAConfigConstants
//import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
//import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
//import org.apache.spark.sql.catalyst.expressions.odb.common.shape.{Point, Rectangle}
//import org.apache.spark.sql.execution.odb.index.hnsw.RefHnsw
//import org.apache.spark.sql.execution.odb.index.local.RTree
//import org.apache.spark.util.SizeEstimator
//import com.qt.kahip.KaHIPWrapper
//import org.apache.spark.sql.execution.odb.index.MVPTree.MVPTree
//import org.apache.spark.sql.execution.odb.index.entity.MVPDP
//
//import scala.collection.JavaConverters.seqAsJavaListConverter
//import scala.collection.mutable
//import scala.math.{max, min, pow}
//import scala.collection.mutable.ArrayBuffer
//import scala.util.Random
//
//case class Bounds(min: Array[Double], max: Array[Double])
//
//case class GlobalAdaptivePartitioner(RtreeNumOfPartition: Int,
//                                     HNSWNumOfPartition: Int,
//                                     RtreeMaxEntriesPerNode: Int,
//                                     sampleRate: Double,
//                                     minSampleSize: Long,
//                                     metricData: Array[MetricData]) extends Partitioner {
//  val metricM = getMetricM()
//  //  val BPlusTree = buildBPlusTree()
//  //  val fineGrainedPoint = cutMetricDataToPoint()
//  val rTreeForest = new Array[RTree](metricM.length)
//  val mvpTreeForest = new Array[MVPTree](metricM.length)
//  //  val hnswForest = new Array[RefHnsw](metricM.length)
//  //  val sampleHnswForest = new Array[RefHnsw](metricM.length)
//  val sampleDataForest = new Array[Array[Point[Any]]](metricM.length)
//  val kahipArray = new ArrayBuffer[Array[Int]](metricM.length)
//  var dataShuffled: Array[Array[_ <: Array[Point[Any]]]] = _
//  val distanceOrd = Ordering.Double
//  val bf = 2
//  val p = 20
//  val k = 25
//  //  val PQArray = calPQArray()
//
//  override def numPartitions: Int = 1
//
//  override def getPartition(key: Any): Int = {
//    key match {
//      case point: Point[Any] =>
//        val metricIndex = point.metricIndex
//        val metricValue = point.metricValue
//        metricValue match {
//          case 0 | 1 | 4 =>
//            // Rtree
//            val k = point
//            val partitions = rTreeForest(metricIndex).circleRange(k, 0.0)
//            partitions((k.toString.hashCode % partitions.length +
//              partitions.length) % partitions.length)._2
//          //          case 3 =>
//          //            // HNSW
//          //            // find the nearest point in samplePoints
//          //            val nearest = sampleHnswForest(metricIndex).knn(point, 1).head
//          //            val nearestIndex = sampleDataForest(metricIndex).indexOf(nearest)
//          //            kahipArray(metricIndex)(nearestIndex)
//        }
//    }
//  }
//
//  //  private def calPQArray(key: MetricData): Array[Int] = {
//  //
//  //    metricM.zipWithIndex.map {
//  //      metricValue =>
//  //        metricValue._2 match {
//  //          case 0 | 1 | 4 =>
//  //            // Rtree
//  //            val k = key.points(metricValue._1)
//  //            val partitions = rTreeForest(metricValue._1).circleRange(k, 0.0)
//  //            partitions((k.toString.hashCode % partitions.length +
//  //              partitions.length) % partitions.length)._2
//  //          case 2 | 3 =>
//  //            // HNSW
//  //            // find the nearest point in samplePoints
//  //            val point = key.points(metricValue._1)
//  //            val nearest = sampleHnswForest(metricValue._1).knn(point, 1).head
//  //            val nearestIndex = sampleDataForest(metricValue._1).indexOf(nearest)
//  //            kahipArray(metricValue._1)(nearestIndex)
//  //        }
//  //
//  //    }
//  //  }
//
//  //  var dataShuffled: Array[Array[Point[Any]]] = _
//  private def getMetricM(): Array[Int] = {
//    metricData.take(1).head.points.map(x => x.metricValue)
//  }
//
//  def getData(totalCount: Long): Array[MetricData] = {
//    if (totalCount <= minSampleSize) {
//      metricData
//    } else {
//      metricData.sample(withReplacement = false, sampleRate, seed).collect()
//    }
//  }
//
//
//  private def buildAdaptiveTree(): Unit = {
//    metricM.zipWithIndex.foreach {
//      case (metricValue, metricIndex) =>
//        metricValue match {
//          case 0 | 1 | 4 =>
//            // Rtree
//            val mbrBounds = calMbrBounds(metricIndex, metricValue)
//            if (mbrBounds.nonEmpty) {
//              rTreeForest(metricIndex) = RTree(mbrBounds.map(x => (x._1, x._2, 1)), RtreeMaxEntriesPerNode)
//            } else {
//              null
//            }
//          case 2 =>
//            mvpTreeForest(metricIndex) = new MVPTree(bf, p, k, 0, 0, 0, 0, null)
//            val mvpdpArray = metricData.zipWithIndex.map {
//              case (data, index) =>
//                new MVPDP(index, data.points(metricIndex))
//            }
//            mvpTreeForest(metricIndex).mvpAdd(mvpdpArray.toList.asJava, metricData.length)
//
//          //          case 3 =>
//          //            // sample HNSW
//          //            sampleHnswForest(metricIndex) = new RefHnsw(distanceOrd)
//          //            val sampleData = getData(metricData.length)
//          //            sampleDataForest(metricIndex) = sampleData.map(_.points(metricIndex))
//          //            sampleHnswForest(metricIndex).allocate(sampleData.length)
//          //            sampleData.foreach(x => sampleHnswForest(metricIndex).add(x.points(metricIndex)))
//          //            val (xadj, adjncy) = sampleHnswForest(metricIndex).adjacencyListToCSR()
//          //            val n = xadj.length - 1
//          //            val vwgt = Array[Int]()
//          //            val adjcwgt = Array[Int]()
//          //            val suppress_output = false
//          //            val imbalance = 0.4
//          //            val seed = 123456
//          //            val mode = 0
//          //            val result = KaHIPWrapper.kaffpa(n, vwgt, xadj, adjcwgt, adjncy, HNSWNumOfPartition, imbalance, suppress_output, seed, mode)
//          //            kahipArray(metricIndex) = result.getPart
//
//
//          // HNSW
//          //            val hnsw = new RefHnsw(distanceOrd)
//          //            hnsw.allocate(metricData.length)
//          //            metricData.foreach(x => hnsw.add(x.points(metricIndex)))
//          //            hnswForest(metricIndex) = hnsw
//          // QT: Sample or not
//
//
//        }
//
//    }
//  }
//
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
//
//  def getBoundsAndCount(metricIndex: Int, metricValue: Int): (Bounds, Long) = {
//    metricData.aggregate[(Bounds, Long)]((null, 0))((bound, data) => {
//      val new_bound = if (bound._1 == null) {
//        Bounds(data.points(metricIndex).coord.asInstanceOf[Array[Double]], data.points(metricIndex).coord.asInstanceOf[Array[Double]])
//      } else {
//        Bounds(bound._1.min.zip(data.points(metricIndex).coord.asInstanceOf[Array[Double]]).map(x => Math.min(x._1, x._2)),
//          bound._1.max.zip(data.points(metricIndex).coord.asInstanceOf[Array[Double]]).map(x => Math.max(x._1, x._2)))
//      }
//      (new_bound, bound._2 + SizeEstimator.estimate(data))
//    }, (left, right) => {
//      val new_bound = {
//        if (left._1 == null) {
//          right._1
//        } else if (right._1 == null) {
//          left._1
//        } else {
//          Bounds(left._1.min.zip(right._1.min).map(x => Math.min(x._1, x._2)),
//            left._1.max.zip(right._1.max).map(x => Math.max(x._1, x._2)))
//        }
//      }
//      (new_bound, left._2 + right._2)
//    })
//  }
//
//  def recursiveGroupPoint(dimensionCount: Array[Int], dataBounds: Bounds,
//                          entries: Array[MetricData], metricIndex: Int, metricValue: Int, currentBounds: Bounds,
//                          currentDimension: Int, untilDimension: Int): Array[Rectangle] = {
//    var ans = mutable.ArrayBuffer[Rectangle]()
//    if (entries.isEmpty) {
//      return ans.toArray
//    }
//
//    val len = entries.length.toDouble
//    val grouped = entries.sortWith(_.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//        < _.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension))
//      .grouped(Math.ceil(len / dimensionCount(currentDimension)).toInt).toArray
//    if (currentDimension < untilDimension) {
//      for (i <- grouped.indices) {
//        if (i == 0 && i == grouped.length - 1) {
//          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
//          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
//        } else if (i == 0) {
//          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
//          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//        } else if (i == grouped.length - 1) {
//          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
//        } else {
//          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//        }
//        ans ++= recursiveGroupPoint(dimensionCount, dataBounds, grouped(i), metricIndex, metricValue,
//          currentBounds, currentDimension + 1, untilDimension)
//      }
//      ans.toArray
//    } else {
//      for (i <- grouped.indices) {
//        if (i == 0 && i == grouped.length - 1) {
//          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
//          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
//        } else if (i == 0) {
//          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
//          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//        } else if (i == grouped.length - 1) {
//          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
//        } else {
//          currentBounds.min(currentDimension) = grouped(i).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//          currentBounds.max(currentDimension) = grouped(i + 1).head.points(metricIndex).coord.asInstanceOf[Array[Double]](currentDimension)
//        }
//        ans += Rectangle(Point[Any](currentBounds.min.clone(), metricIndex, metricValue),
//          Point[Any](currentBounds.max.clone(), metricIndex, metricValue))
//      }
//      ans.toArray
//    }
//  }
//
//
//  //  private def cutMetricDataToPoint(): Array[Array[Point[Any]]] = {
//  //    val demoPoint = metricData.take(1).head.points
//  //    val dim = demoPoint.length
//  //    val fineGrainedPoint = new Array[Array[Point[Any]]](dim)
//  //      .zipWithIndex.map(
//  //        x => {
//  //          val index = x._2
//  //          val points = metricData.map(
//  //            y => {
//  //              y.points(index)
//  //            }
//  //          )
//  //          points
//  //        }
//  //      )
//  //    fineGrainedPoint
//  //  }
//
//
//}
//
//object GlobalAdaptivePartitioner {
//  private val minSampleSize = ODBConfigConstants.MIN_SAMPLE_SIZE
//  private val sampleRate = ODBConfigConstants.SAMPLE_RATE
//
//  private val maxEntriesPerNode = ODBConfigConstants.RTREE_GLOBAL_MAX_ENTRIES_PER_NODE
//  private val numOfPartition = ODBConfigConstants.RTREE_GLOBAL_NUM_PARTITIONS
//  private val numHNSWPartition = ODBConfigConstants.NUM_HNSW_PARTITIONS
//
//  def partition(metricData: Array[MetricData]):
//  (Array[Array[MetricData]], GlobalAdaptivePartitioner) = {
//
//    val partitioner = new GlobalAdaptivePartitioner(numOfPartition, numHNSWPartition, maxEntriesPerNode, sampleRate, minSampleSize, metricData)
//    partitioner.buildAdaptiveTree()
//    val dataShuffled = partitioner.metricM.zipWithIndex.map {
//      case (metricValue, metricIndex) =>
//        metricValue match {
//          case 0 | 1 | 3 | 4 =>
//            val shuffled = metricData.map(x => x.points(metricIndex)).groupBy(p => partitioner.getPartition(p))
//            (0 until partitioner.numPartitions).map(i =>
//              shuffled.getOrElse(i, Array.empty)).toArray
//          case _ =>
//            Array.empty
//        }
//    }
//    //    val shuffled = metricData.groupBy(p => partitioner.getPartition(p))
//    //
//    //    val dataShuffled = (0 until partitioner.numPartitions).map(i =>
//    //      shuffled.getOrElse(i, Array.empty)).toArray
//    partitioner.dataShuffled = dataShuffled
//    (null, partitioner)
//  }
//
//}
