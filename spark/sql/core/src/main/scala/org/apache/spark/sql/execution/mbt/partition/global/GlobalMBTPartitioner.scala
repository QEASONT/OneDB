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

package org.apache.spark.sql.execution.mbt.partition.global

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.expressions.mbt.MBTSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants
import org.apache.spark.sql.catalyst.expressions.mbt.common.shape.Point
import org.apache.spark.sql.execution.mbt.index.global.MTree
import org.apache.spark.sql.execution.mbt.index.global.Entry
import org.apache.spark.util.SizeEstimator

import scala.collection.mutable.ArrayBuffer

case class GlobalMBTPartitioner(@transient data:
                                RDD[_ <: Product2[Point[Any], Any]],
                                sampleSize: Long,
                                distanceFunction: MBTSimilarityFunction
                               ) extends Partitioner {
  val MTree = buildMTree()

  override def numPartitions: Int = calNumPartition() + 1

  override def getPartition(key: Any): Int = {
    if (MTree == null) {
      -2
    } else {
      val k = key.asInstanceOf[Point[Any]]
      val e = new Entry[Int](k, 0, 0);
      val partitionNum = MTree.getSampleLeafIndex(e, MTree.root)
      //      println(partitionNum)
      partitionNum
    }
  }

  def calNumPartition(): Int = {
    val numPartition = MTree.calNumPartition(MTree.root)
    numPartition
  }

  private def buildMTree(): MTree[Int] = {
    val MTree = new MTree[Int](InnerOrder = MBTConfigConstants.M_TREE_INNER_ORDER,
      LeafOrder = MBTConfigConstants.M_TREE_LEAF_ORDER, distanceFunction = distanceFunction)
    val totalCount = SizeEstimator.estimate(data)
    val dataArray = getData(totalCount)
    dataArray.foreach(point => {
      val e = new Entry[Int](point, 0, 0);
      MTree.insert(e)
    })
    MTree.setLeafIndex(MTree.root, 0)
    MTree.calChildrenNum(MTree.root)
    MTree
  }

  def getData(totalCount: Long): Array[Point[Any]] = {
    val seed = System.currentTimeMillis()
    if (totalCount <= sampleSize) {
      data.map(_._1).collect()
      //    } else if (totalCount * sampleRate <= maxSampleSize) {
      //      data.sample(withReplacement = false, sampleRate, seed).map(_._1).collect()
    } else {
      data.sample(withReplacement = false,
        sampleSize.toDouble / totalCount, seed).map(_._1).collect()
    }
  }

  def getDistanceFromParent(point: Point[Any]): Double = {
    val e = new Entry[Int](point, 0, 0);
    MTree.getQueryDistanceFromParent(e)
  }

  def getPartitionsWithThreshold(key: Point[Any], threshold: Double): List[Int] = {
    MTree.indexRangeSearch(key, threshold).toList.distinct
  }

  def getQueryDistanceFromParent(key: Point[Any]): Double = {
    val e = new Entry[Int](key, 0, 0);
    MTree.getQueryDistanceFromParent(e)
  }

  def getEstimatedKNNDist(key: Point[Any], k: Int): Double = {
    //    MTree.estimatedKNNDist(key, k)
    MTree.kNNSearch(key, k).last._2
  }
}

object GlobalMBTPartitioner {
  private val sampleSize = MBTConfigConstants.SAMPLE_SIZE

  def partition(dataRDD: RDD[Point[Any]], distanceFunction: MBTSimilarityFunction):
  (RDD[Point[Any]], GlobalMBTPartitioner) = {
    //    val totalCluster = MBTConfigConstants.M_TREE_ORDER
    // TODO:QT collect?

    // shuffle
    val pairedDataRDD = dataRDD.map(x => {
      (x, None)
    })


    val partitioner = new GlobalMBTPartitioner(pairedDataRDD, sampleSize, distanceFunction)

    //    val aaa = dataRDD.collect().map(x => partitioner.getPartition(x)).foreach(println)
    // QT: shuffled key value pair = [Point[Any], Any,Any]?
    //    val shuffled = new ShuffledRDD[Int, Point[Any], Any](pairedDataRDD, partitioner)

    val shuffled = new ShuffledRDD[Point[Any], Any, Any](pairedDataRDD, partitioner)
    //    val bbb = shuffled.collect()
    (shuffled.map(_._1), partitioner)
  }


}
