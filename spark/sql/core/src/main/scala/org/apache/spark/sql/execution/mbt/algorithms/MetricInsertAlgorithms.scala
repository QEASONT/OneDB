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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.mbt.PackedPartition
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants
import org.apache.spark.sql.catalyst.expressions.mbt.common.shape.Point
import org.apache.spark.sql.execution.mbt.index.global.{GlobalBPlusTreeIndex, GlobalMBTIndex}
import org.apache.spark.sql.execution.mbt.index.local.LocalMTreeIndex
import org.apache.spark.sql.execution.mbt.rdd.MBTRDD

object MetricInsertAlgorithms {
  // search from M-tree
  def localSearchMode5(query: Point[Any], packedPartition: PackedPartition,
                       threshold: Double):
  Iterator[(Point[Any], Double)] = {
    val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalMTreeIndex]).head
      .asInstanceOf[LocalMTreeIndex]
    val answers = localIndex.getResultWithThreshold(query, threshold).iterator
    answers
  }

  def localInsert(query: Point[Any], packedPartition: PackedPartition):
  Iterator[(Point[Any], Double)] = {
    val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalMTreeIndex]).head
      .asInstanceOf[LocalMTreeIndex]
    val res = localIndex.insertPoint(query).iterator
    res
  }


  object DistributedSearch extends Logging {
    def search(sparkContext: SparkContext, query: Point[Any], mbtRDD: MBTRDD):
    RDD[(Point[Any], Double)] = {
      val bQuery = sparkContext.broadcast(query)

      MBTConfigConstants.MBT_MODE match {
        case 3 =>
          val globalMBTIndex = mbtRDD.globalIndex.asInstanceOf[GlobalMBTIndex]
          val globalBPlusTreeIndex = mbtRDD.globalBPlusTreeIndex.asInstanceOf[GlobalBPlusTreeIndex]

          var start = System.currentTimeMillis()
          var end = start
          val distanceFromParent = globalMBTIndex.getQueryDistanceFromParent(bQuery.value)
          end = System.currentTimeMillis()
          logWarning(s"MBT Get distanceFromParent: ${end - start} ms")

          start = System.currentTimeMillis()
          end = start
          val globalCandidatePartitions = List(globalBPlusTreeIndex.
            getPartition((distanceFromParent, bQuery.value)))
          end = System.currentTimeMillis()
          logWarning(s"MBT Get candidatePartitions: ${end - start} ms")

          start = System.currentTimeMillis()
          end = start
          val answers = PartitionPruningRDD.create(mbtRDD.globalBPlusRDD,
            globalCandidatePartitions.contains)
            .flatMap(packedPartition => localInsert(bQuery.value, packedPartition))
          end = System.currentTimeMillis()
          // check whether contain true
          answers
      }
    }
  }
}

