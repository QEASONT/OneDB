package org.apache.spark.sql.execution.odb.partition.local

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
//package org.apache.spark.sql.execution.odb.partition.local
//
//import org.apache.spark.Partitioner
//import org.apache.spark.rdd.{RDD, ShuffledRDD}
//import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
//import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
//import org.apache.spark.sql.execution.odb.index.local.MemoryBPlusTree
//
//import scala.math.{max, min}
//import scala.collection.mutable.ArrayBuffer
//
//case class LocalBPlusPartitioner(dataKeyValuePair: Array[(Double, Point)]) extends Partitioner {
//
//  val BPlusTree = buildBPlusTree()
//  var dataShuffled: Array[Array[Point]] = _
//
//  private def buildBPlusTree(): MemoryBPlusTree[Double, Any] = {
//    val bPlusTree = new MemoryBPlusTree[Double, Any](ODBConfigConstants.BPlusTreeOrder)
//    dataKeyValuePair.foreach(x => bPlusTree.insertKeys(x._1))
//    bPlusTree.setLeafIndex()
//    bPlusTree
//  }
//
//  override def numPartitions: Int = BPlusTree.getNumLeaf
//
//  override def getPartition(key: Any): Int = {
//    val k = key.asInstanceOf[(Double, Point)]
//    val exactPartition = BPlusTree.search(k._1).get
//    exactPartition.asInstanceOf[Int]
//  }
//
//  def searchBPlusTree(lowerBound: Int, upperBound: Int):
//  List[Point] = {
//    val lowerBoundReturn1 = BPlusTree.greatestLessThan(lowerBound.toInt)
//    val upperBoundReturn1 = BPlusTree.leastGreaterThan(upperBound.toInt)
//    val lowerBoundReturn2 = BPlusTree.greatestLessThan(lowerBound.toInt)
//    val upperBoundReturn2 = BPlusTree.leastGreaterThan(upperBound.toInt)
//
//    var lowerIndex = -1
//    var upperIndex = -1
//
//    if (!lowerBoundReturn1.isEmpty) {
//      lowerIndex = lowerBoundReturn1.get._2.asInstanceOf[Int]
//    }
//    else if (!lowerBoundReturn2.isEmpty) {
//      lowerIndex = lowerBoundReturn2.get._2.asInstanceOf[Int]
//    }
//    else {
//      lowerIndex = -1
//    }
//    if (!upperBoundReturn2.isEmpty) {
//      upperIndex = upperBoundReturn2.get._2.asInstanceOf[Int]
//    }
//    else if (!upperBoundReturn1.isEmpty) {
//      upperIndex = upperBoundReturn1.get._2.asInstanceOf[Int]
//    }
//    else {
//      upperIndex = -1
//    }
//
//    if (lowerIndex != (-1) && upperIndex != (-1)) {
//      if (lowerIndex < upperIndex) {
//        val candidates: ArrayBuffer[Point] = ArrayBuffer[Point]()
//        for (i <- lowerIndex until (upperIndex + 1)) {
//          candidates ++= dataShuffled(i)
//        }
//        candidates.toList
//      }
//      else {
//        val candidates = dataShuffled(upperIndex).toList
//        candidates
//      }
//    }
//    else if (lowerIndex != (-1)) {
//      val candidates = dataShuffled(lowerIndex).toList
//      candidates
//    }
//    else if (upperIndex != (-1)) {
//      val candidates = dataShuffled(upperIndex).toList
//      candidates
//    }
//    else {
//      List.empty
//    }
//  }
//
//
//  def getCandidatesWithThreshold(key: Point, threshold: Double,
//                                 distanceFromParent: Double): List[Point] = {
//    val lowerBound = max(0, distanceFromParent - threshold)
//    val upperBound = distanceFromParent + threshold
//    searchBPlusTree(lowerBound.toInt, upperBound.toInt)
//  }
//}
//
//object LocalBPlusPartitioner {
//  def partition(dataKeyValuePair: Array[(Double, Point)]):
//  (Array[Array[Point]], LocalBPlusPartitioner) = {
//    val partitioner = new (dataKeyValuePair)
//    val shuffled = dataKeyValuePair.groupBy(p => partitioner.getPartition(p))
//    val dataShuffled = (0 until partitioner.numPartitions).map(i =>
//      shuffled.getOrElse(i, Array.empty).map(x => x._2)).toArray
//    partitioner.dataShuffled = dataShuffled
//    (dataShuffled, partitioner)
//  }
//}