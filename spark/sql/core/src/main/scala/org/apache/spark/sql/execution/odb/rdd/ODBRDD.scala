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

package org.apache.spark.sql.execution.odb.rdd

import com.codahale.metrics.Metric
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.expressions.odb.{ODBSimilarityFunction, PackedPartition}
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.index.{GlobalIndex, LocalIndex}
import org.apache.spark.sql.execution.odb.index.global.GlobalODBIndex
import org.apache.spark.sql.execution.odb.index.local.LocalODBIndex
import org.apache.spark.sql.execution.odb.partition.global.{ExactKeyPartitioner, GlobalBPlusTreePartitioner, GlobalODBPartitioner}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class ODBRDD(dataRDD: RDD[MetricData]) extends Serializable {
  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val metricDataRDD = dataRDD
  var packedRDD: RDD[PackedPartition] = _
  var globalODBIndex: GlobalIndex = _
  //  var metricDataRDD: RDD[MetricData] = _

  private def buildIndex(): Unit = {

    var start = System.currentTimeMillis()
    var end = start


    val (partitionedRDD, partitioner) = GlobalODBPartitioner.partition(dataRDD)
    end = System.currentTimeMillis()
    LOG.warn(s"ODB Global ODB Partitioning Time: ${end - start} ms")

    start = System.currentTimeMillis()

    packedRDD = partitionedRDD.mapPartitionsWithIndex {
      case (index, iter) =>
        val data = iter.toArray
        val indexes = ArrayBuffer.empty[LocalIndex]
        indexes.append(LocalODBIndex.buildIndex(data))
        Array(PackedPartition(index, data, indexes.toArray)).iterator
    }
    packedRDD.persist(StorageLevel.MEMORY_ONLY).count()
    end = System.currentTimeMillis()
    LOG.warn(s"Building Local Index Time: ${end - start} ms")
    // log statistics
    val partitionSizes = packedRDD.mapPartitions(iter => iter.map(_.data.length)).collect()
    LOG.warn(s"Tree Partitions Count: ${partitionSizes.length}")
    LOG.warn(s"Tree Partitions Sizes: ${partitionSizes.mkString(",")}")
    LOG.warn(s"Max Partition Size: ${partitionSizes.max}")
    LOG.warn(s"Min Partition Size: ${partitionSizes.min}")
    LOG.warn(s"Avg Partition Size: ${partitionSizes.sum / partitionSizes.length}")
    val sortedPartitionSizes = partitionSizes.sorted
    LOG.warn(s"5% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.05).toInt)}")
    LOG.warn(s"25% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.25).toInt)}")
    LOG.warn(s"50% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.5).toInt)}")
    LOG.warn(s"75% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.75).toInt)}")
    LOG.warn(s"95% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.95).toInt)}")
    // build global index
    start = System.currentTimeMillis()
    globalODBIndex = GlobalODBIndex(partitioner)
    end = System.currentTimeMillis()
    LOG.warn(s"Building Global Index Time: ${end - start} ms")
  }

  buildIndex()
}