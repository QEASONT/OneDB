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
package org.apache.spark.sql.execution.mbt.algorithms

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.mbt.PackedPartition
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants
import org.apache.spark.sql.catalyst.expressions.mbt.common.metric.MBTSimilarity
import org.apache.spark.sql.catalyst.expressions.mbt.common.shape.Point
import org.apache.spark.sql.execution.mbt.index.global.{GlobalBPlusTreeIndex, GlobalMBTIndex}
import org.apache.spark.sql.execution.mbt.index.local.LocalMBTIndex
import org.apache.spark.sql.execution.mbt.index.local.LocalMTreeIndex
import org.apache.spark.sql.execution.mbt.index.local.LocalSimpleIndex
import org.apache.spark.sql.execution.mbt.index.local.LocalPivotIndex
import org.apache.spark.sql.execution.mbt.rdd.MBTRDD

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

object MetricSimilarityWithKNNAlgorithms {

  object DistributedSearch extends Logging {
    implicit val order = new Ordering[(Point[Any], Double)] {
      def compare(x: (Point[Any], Double), y: (Point[Any], Double)): Int = {
        x._2.compare(y._2)
      }
    }

    def localSearchMode5(query: Point[Any], packedPartition: PackedPartition,
                         threshold: Double):
    Iterator[(Point[Any], Double)] = {

      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalMTreeIndex]).head
        .asInstanceOf[LocalMTreeIndex]
      val answers = localIndex.getResultWithThreshold(query, threshold).iterator
      answers
    }
//
//    def localSearchMode6(query: Point[Any], packedPartition: PackedPartition,
//                         threshold: Double):
//    Iterator[(Point[Any], Double)] = {
//
//      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalSimpleIndex]).head
//        .asInstanceOf[LocalSimpleIndex]
//      val answers = localIndex.getResultWithThreshold(query, threshold).iterator
//      answers
//    }
//
//    def localSearchMode7(query: Point[Any], packedPartition: PackedPartition,
//                         threshold: Double):
//    Iterator[(Point[Any], Double)] = {
//
//      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalPivotIndex]).head
//        .asInstanceOf[LocalPivotIndex]
//      val answers = localIndex.getResultWithThreshold(query, threshold).iterator
//      answers
//    }
//
//    def localSearchMode3(query: Point[Any], packedPartition: PackedPartition,
//                         threshold: Double):
//    Iterator[(Point[Any], Double)] = {
//      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalMTreeIndex]).head
//        .asInstanceOf[LocalMTreeIndex]
//      val answers = localIndex.getCandidatesWithThreshold(query, threshold)
//        .map(x => (x, x.minDist(query))).filter(x => x._2 <= threshold).iterator
//      answers
//    }
//
//    def localSearchMode2(query: Point[Any], packedPartition: PackedPartition,
//                         threshold: Double, distanceFromParent: Double):
//    List[Int] = {
//      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalMBTIndex]).head
//        .asInstanceOf[LocalMBTIndex]
//      val candidatesIndex = localIndex.getCandidatesIntWithThreshold(query,
//        threshold, distanceFromParent)
//      // TODO:QT???RDD RDD??
//      //    val answers = PartitionPruningRDD.create(newPackedRDD, candidatesIndex.contains)
//      candidatesIndex
//    }
//
//    def localSearchMode1(query: Point[Any], packedPartition: PackedPartition,
//                         threshold: Double, distanceFromParent: Double):
//    Iterator[(Point[Any], Double)] = {
//      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalMBTIndex]).head
//        .asInstanceOf[LocalMBTIndex]
//      val answers = localIndex.getCandidatesWithThreshold(query, threshold, distanceFromParent)
//        .map(x => (x, x.minDist(query)))
//        .filter(x => x._2 <= threshold).iterator
//      answers
//    }

    def search(sparkContext: SparkContext, query: Point[Any], mbtRDD: MBTRDD,
               count: Int): RDD[(Point[Any], Double)] = {

      val bQuery = sparkContext.broadcast(query)

      MBTConfigConstants.MBT_MODE match {
//        case 1 =>
//          val globalMBTIndex = mbtRDD.globalIndex.asInstanceOf[GlobalMBTIndex]
//
//          var start = System.currentTimeMillis()
//          var end = start
//          val estimatedDist = globalMBTIndex.getEstimatedKNNDist(bQuery.value, count)
//          val globalCandidatePartitions = globalMBTIndex.getPartitionsWithThreshold(bQuery.value,
//            estimatedDist)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get candidatePartitions: ${end - start} ms")
//
//          start = System.currentTimeMillis()
//          end = start
//          val distanceFromParent = globalMBTIndex.getQueryDistanceFromParent(bQuery.value)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get distanceFromParent: ${end - start} ms")
//          val res = PartitionPruningRDD.create(mbtRDD.packedRDD, globalCandidatePartitions.contains)
//            .flatMap(packedPartition => localSearchMode1(bQuery.value, packedPartition, estimatedDist, distanceFromParent)).distinct().takeOrdered(count)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get Result of LocalSearch: ${end - start} ms")
//          sparkContext.parallelize(res)
//        case 2 =>
//          val globalMBTIndex = mbtRDD.globalIndex.asInstanceOf[GlobalMBTIndex]
//
//          var start = System.currentTimeMillis()
//          var end = start
//          val estimatedDist = globalMBTIndex.getEstimatedKNNDist(bQuery.value, count)
//          val globalCandidatePartitions = globalMBTIndex.getPartitionsWithThreshold(bQuery.value,
//            estimatedDist)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get candidatePartitions: ${end - start} ms")
//
//          start = System.currentTimeMillis()
//          end = start
//          val distanceFromParent = globalMBTIndex.getQueryDistanceFromParent(bQuery.value)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get distanceFromParent: ${end - start} ms")
//          val hashPartitioner = new HashPartitioner(40)
//          val localCandidatesPartition = PartitionPruningRDD
//            .create(mbtRDD.packedRDD, globalCandidatePartitions.contains)
//            .flatMap(packedPartition =>
//              localSearchMode2(bQuery.value, packedPartition, estimatedDist, distanceFromParent)
//                .map(x => hashPartitioner.getPartition(packedPartition.id.toString + "," + x.toString)))
//
//          val answers = PartitionPruningRDD.create(mbtRDD.newPackedRDD,
//            localCandidatesPartition.collect().toList.contains).map(x => (x, x.minDist(query)))
//            .distinct().takeOrdered(count)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get Result of LocalSearch: ${end - start} ms")
//          sparkContext.parallelize(answers)

        case 3 =>
          val globalMBTIndex = mbtRDD.globalIndex.asInstanceOf[GlobalMBTIndex]
          val globalBPlusTreeIndex = mbtRDD.globalBPlusTreeIndex.asInstanceOf[GlobalBPlusTreeIndex]

          var start = System.currentTimeMillis()
          var end = start
          val estimatedDist = globalMBTIndex.getEstimatedKNNDist(bQuery.value, count)
          val distanceFromParent = globalMBTIndex.getQueryDistanceFromParent(bQuery.value)

          start = System.currentTimeMillis()
          end = start
          val globalCandidatePartitions = globalBPlusTreeIndex.
            getPartitionsWithThreshold(bQuery.value, estimatedDist, distanceFromParent)
          end = System.currentTimeMillis()
          logWarning(s"MBT Get candidatePartitions: ${end - start} ms")

          start = System.currentTimeMillis()
          end = start
          val answers = PartitionPruningRDD.create(mbtRDD.globalBPlusRDD,
            globalCandidatePartitions.contains)
            .flatMap(packedPartition => localSearchMode5(bQuery.value, packedPartition, estimatedDist)).distinct()
          //          val cc = answers.takeOrdered(count)
          end = System.currentTimeMillis()
          logWarning(s"MBT Get Result of LocalSearch: ${end - start} ms")
          sparkContext.parallelize(answers.takeOrdered(count))
//        case 4 =>
//          val globalMBTIndex = mbtRDD.globalIndex.asInstanceOf[GlobalMBTIndex]
//
//          var start = System.currentTimeMillis()
//          var end = start
//          val estimatedDist = globalMBTIndex.getEstimatedKNNDist(bQuery.value, count)
//          val globalCandidatePartitions = globalMBTIndex.getPartitionsWithThreshold(bQuery.value,
//            estimatedDist)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get candidatePartitions: ${end - start} ms")
//
//          start = System.currentTimeMillis()
//          val distanceFromParent = globalMBTIndex.getQueryDistanceFromParent(bQuery.value)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get distanceFromParent: ${end - start} ms")
//
//          //          val numLocalPartition = MBTConfigConstants.MBT4_LOCAL_PARTITION
//          //          val hashPartitioner = new HashPartitioner(numLocalPartition)
//          start = System.currentTimeMillis()
//          val localCandidatesPartition = PartitionPruningRDD
//            .create(mbtRDD.packedRDD, globalCandidatePartitions.contains)
//            .flatMap(packedPartition =>
//              localSearchMode2(bQuery.value, packedPartition, estimatedDist, distanceFromParent))
//            .distinct()
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get Local BPlusTree Candidates(Mode2): ${end - start} ms")
//
//          val answers = PartitionPruningRDD.create(mbtRDD.finalPackedRDD,
//            localCandidatesPartition.collect().toList.contains)
//            .flatMap(packedPartition =>
//              localSearchMode3(bQuery.value, packedPartition, estimatedDist)
//            ).distinct().takeOrdered(count)
//
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get Result of LocalSearch(Mode3): ${
//            end - start
//          } ms")
//          sparkContext.parallelize(answers)
//        case 5 =>
//          val globalMBTIndex = mbtRDD.globalIndex.asInstanceOf[GlobalMBTIndex]
//          val globalBPlusTreeIndex = mbtRDD.globalBPlusTreeIndex.asInstanceOf[GlobalBPlusTreeIndex]
//
//          var start = System.currentTimeMillis()
//          var end = start
//          val estimatedDist = globalMBTIndex.getEstimatedKNNDist(bQuery.value, count)
//          val distanceFromParent = globalMBTIndex.getQueryDistanceFromParent(bQuery.value)
//
//          start = System.currentTimeMillis()
//          end = start
//          val globalCandidatePartitions = globalBPlusTreeIndex.
//            getPartitionsWithThreshold(bQuery.value, estimatedDist, distanceFromParent)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get candidatePartitions: ${end - start} ms")
//
//          start = System.currentTimeMillis()
//          end = start
//          val answers = PartitionPruningRDD.create(mbtRDD.globalBPlusRDD,
//            globalCandidatePartitions.contains)
//            .flatMap(packedPartition => localSearchMode6(bQuery.value, packedPartition, estimatedDist)).distinct()
//          //          val cc = answers.takeOrdered(count)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get Result of LocalSearch: ${end - start} ms")
//          sparkContext.parallelize(answers.takeOrdered(count))
//        case 6 =>
//          val globalMBTIndex = mbtRDD.globalIndex.asInstanceOf[GlobalMBTIndex]
//          val globalBPlusTreeIndex = mbtRDD.globalBPlusTreeIndex.asInstanceOf[GlobalBPlusTreeIndex]
//
//          var start = System.currentTimeMillis()
//          var end = start
//          val estimatedDist = globalMBTIndex.getEstimatedKNNDist(bQuery.value, count)
//          val distanceFromParent = globalMBTIndex.getQueryDistanceFromParent(bQuery.value)
//
//          start = System.currentTimeMillis()
//          end = start
//          val globalCandidatePartitions = globalBPlusTreeIndex.
//            getPartitionsWithThreshold(bQuery.value, estimatedDist, distanceFromParent)
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get candidatePartitions: ${end - start} ms")
//
//          start = System.currentTimeMillis()
//          end = start
//          val answers = PartitionPruningRDD.create(mbtRDD.globalBPlusRDD,
//            globalCandidatePartitions.contains)
//            .flatMap(packedPartition => localSearchMode7(bQuery.value, packedPartition, estimatedDist)).distinct()
//          end = System.currentTimeMillis()
//          logWarning(s"MBT Get Result of LocalSearch: ${end - start} ms")
//          sparkContext.parallelize(answers.takeOrdered(count))
      }
    }
  }
}
