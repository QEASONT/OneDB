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

package org.apache.spark.sql.execution.odb.algorithms

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.odb.PackedPartition
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.index.global.{GlobalBPlusTreeIndex, GlobalODBIndex}
import org.apache.spark.sql.execution.odb.index.local.{LocalMTreeIndex, LocalODBIndex}
import org.apache.spark.sql.execution.odb.rdd.ODBRDD
import org.apache.spark.{HashPartitioner, SparkContext}

object MetricDeleteAlgorithms {

  def localDelete(query: MetricData, packedPartition: PackedPartition):
  Iterator[(MetricData, Double)] = {
    val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalODBIndex]).head
      .asInstanceOf[LocalODBIndex]
    val res = localIndex.deleteMetricData(query).iterator
    res
  }


  object DistributedSearch extends Logging {
    def search(sparkContext: SparkContext, query: MetricData, odbRDD: ODBRDD):
    RDD[(MetricData, Double)] = {
      val bQuery = sparkContext.broadcast(query)
      val globalODBIndex = odbRDD.globalODBIndex.asInstanceOf[GlobalODBIndex]
      var start = System.currentTimeMillis()
      var end = start
      val globalCandidatePartitions = List(globalODBIndex.getPartition(bQuery.value))
      end = System.currentTimeMillis()
      logInfo(s"Time to get global candidate partitions: ${end - start}")
      start = System.currentTimeMillis()
      val answers = PartitionPruningRDD.create(odbRDD.packedRDD, globalCandidatePartitions.contains)
        .flatMap(packedPartition => localDelete(bQuery.value, packedPartition))
      end = System.currentTimeMillis()
      answers

    }
  }
}

