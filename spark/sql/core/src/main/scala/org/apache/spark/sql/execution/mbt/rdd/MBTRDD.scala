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

package org.apache.spark.sql.execution.mbt.rdd

import com.codahale.metrics.Metric
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.expressions.mbt.{MBTSimilarityFunction, PackedPartition}
import org.apache.spark.sql.catalyst.expressions.mbt.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.mbt.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants
import org.apache.spark.sql.catalyst.expressions.mbt.index.{GlobalIndex, LocalIndex}
import org.apache.spark.sql.execution.mbt.index.global.GlobalMBTIndex
import org.apache.spark.sql.execution.mbt.index.global.GlobalBPlusTreeIndex
import org.apache.spark.sql.execution.mbt.index.local.LocalMBTIndex
import org.apache.spark.sql.execution.mbt.index.local.LocalMTreeIndex
import org.apache.spark.sql.execution.mbt.index.local.LocalPivotIndex
import org.apache.spark.sql.execution.mbt.index.local.LocalSimpleIndex
import org.apache.spark.sql.execution.mbt.partition.global.GlobalBPlusTreePartitioner
import org.apache.spark.sql.execution.mbt.partition.global.GlobalMBTPartitioner
import org.apache.spark.sql.execution.mbt.partition.local.KeyPartitioner
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class MBTRDD(dataRDD: RDD[Point[Any]], distanceFunction: MBTSimilarityFunction) extends Serializable {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  var packedRDD: RDD[PackedPartition] = _
  var newPackedRDD: RDD[Point[Any]] = _
  var finalPackedRDD: RDD[PackedPartition] = _
  var globalBPlusRDD: RDD[PackedPartition] = _
  var globalIndex: GlobalIndex = _
  var globalBPlusTreeIndex: GlobalIndex = _
  var numOfLocalPartition: Int = _

  private def buildIndex(): Unit = {
    var start = System.currentTimeMillis()
    var end = start

    // get partition
    start = System.currentTimeMillis()
    val (partitionedRDD, partitioner) = GlobalMBTPartitioner.partition(dataRDD, distanceFunction)
    end = System.currentTimeMillis()
    LOG.warn(s"MBT Partitioning Time1: ${end - start} ms")

    // build local index
    start = System.currentTimeMillis()

    MBTConfigConstants.MBT_MODE match {
      case 3 =>
        //        val aaa = partitionedRDD.getNumPartitions
        LOG.warn(s"Calculating Distance From Parent")
        val keyValueRDD = partitionedRDD.map(x =>
          (partitioner.getDistanceFromParent(x), x))
        //        val aaa = keyValueRDD.collect()
        keyValueRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        keyValueRDD.count()

        LOG.warn(s"Calculate Distance From Parent Down")
        start = System.currentTimeMillis()
        val (bPlusRDD, globalPartitioner) = GlobalBPlusTreePartitioner.partition(keyValueRDD)
        end = System.currentTimeMillis()
        LOG.warn(s"MBT Partitioning Time2: ${end - start} ms")

        start = System.currentTimeMillis()
        //        val aaaa = bPlusRDD.mapPartitionsWithIndex {
        //          case (index, iter) =>
        //            val data = iter.toArray
        //            Array(index, data).iterator
        //        }.collectPartitions()
        globalBPlusRDD = bPlusRDD.mapPartitionsWithIndex {
          case (index, iter) =>
            val data = iter.toArray
            val indexes = ArrayBuffer.empty[LocalIndex]
            indexes.append(LocalMTreeIndex.buildIndex(data, distanceFunction))
            Array(PackedPartition(index, data, indexes.toArray)).iterator
        }
        globalBPlusRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        globalBPlusRDD.count()
        end = System.currentTimeMillis()
        LOG.warn(s"Building Global BPlus Index Time: ${end - start} ms")

        // log statistics
        val partitionSizes = globalBPlusRDD.mapPartitions(iter => iter.map(_.data.length)).collect()
        LOG.warn(s"Local Partitions Count: ${partitionSizes.length}")
        LOG.warn(s"Local Partitions Sizes: ${partitionSizes.mkString(",")}")
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
        val globalTreeIndex = GlobalMBTIndex(partitioner) // Global M-Tree
        val globalBPlusIndex = GlobalBPlusTreeIndex(globalPartitioner) // Global B+-Tree
        globalIndex = globalTreeIndex
        globalBPlusTreeIndex = globalBPlusIndex
        end = System.currentTimeMillis()
        LOG.warn(s"Building Global Index Time: ${end - start} ms")
      case 5 =>
        LOG.warn(s"Calculating Distance From Parent")
        val keyValueRDD = partitionedRDD.map(x =>
          (partitioner.getDistanceFromParent(x), x))
        //        val aaa = keyValueRDD.collect()
        keyValueRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        keyValueRDD.count()

        LOG.warn(s"Calculate Distance From Parent Down")
        start = System.currentTimeMillis()
        val (bPlusRDD, globalPartitioner) = GlobalBPlusTreePartitioner.partition(keyValueRDD)
        end = System.currentTimeMillis()
        LOG.warn(s"MBT Partitioning Time2: ${end - start} ms")

        start = System.currentTimeMillis()
        globalBPlusRDD = bPlusRDD.mapPartitionsWithIndex {
          case (index, iter) =>
            val data = iter.toArray
            val indexes = ArrayBuffer.empty[LocalIndex]
            indexes.append(LocalSimpleIndex.buildIndex(data, distanceFunction))
            Array(PackedPartition(index, data, indexes.toArray)).iterator
        }
        globalBPlusRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        globalBPlusRDD.count()
        end = System.currentTimeMillis()
        LOG.warn(s"Building Global BPlus Index Time: ${end - start} ms")

        // log statistics
        val partitionSizes = globalBPlusRDD.mapPartitions(iter => iter.map(_.data.length)).collect()
        LOG.warn(s"Local Partitions Count: ${partitionSizes.length}")
        LOG.warn(s"Local Partitions Sizes: ${partitionSizes.mkString(",")}")
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
        val globalTreeIndex = GlobalMBTIndex(partitioner) // Global M-Tree
        val globalBPlusIndex = GlobalBPlusTreeIndex(globalPartitioner) // Global B+-Tree
        globalIndex = globalTreeIndex
        globalBPlusTreeIndex = globalBPlusIndex
        end = System.currentTimeMillis()
        LOG.warn(s"Building Global Index Time: ${end - start} ms")
      case 6 =>
        LOG.warn(s"Calculating Distance From Parent")
        val keyValueRDD = partitionedRDD.map(x =>
          (partitioner.getDistanceFromParent(x), x))
        //        val aaa = keyValueRDD.collect()
        keyValueRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        keyValueRDD.count()

        LOG.warn(s"Calculate Distance From Parent Down")
        start = System.currentTimeMillis()
        val (bPlusRDD, globalPartitioner) = GlobalBPlusTreePartitioner.partition(keyValueRDD)
        end = System.currentTimeMillis()
        LOG.warn(s"MBT Partitioning Time2: ${end - start} ms")

        start = System.currentTimeMillis()
        globalBPlusRDD = bPlusRDD.mapPartitionsWithIndex {
          case (index, iter) =>
            val data = iter.toArray
            val indexes = ArrayBuffer.empty[LocalIndex]
            indexes.append(LocalPivotIndex.buildIndex(data, distanceFunction))
            Array(PackedPartition(index, data, indexes.toArray)).iterator
        }
        globalBPlusRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
        globalBPlusRDD.count()
        end = System.currentTimeMillis()
        LOG.warn(s"Building Global BPlus Index Time: ${end - start} ms")

        // log statistics
        val partitionSizes = globalBPlusRDD.mapPartitions(iter => iter.map(_.data.length)).collect()
        LOG.warn(s"Local Partitions Count: ${partitionSizes.length}")
        LOG.warn(s"Local Partitions Sizes: ${partitionSizes.mkString(",")}")
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
        val globalTreeIndex = GlobalMBTIndex(partitioner) // Global M-Tree
        val globalBPlusIndex = GlobalBPlusTreeIndex(globalPartitioner) // Global B+-Tree
        globalIndex = globalTreeIndex
        globalBPlusTreeIndex = globalBPlusIndex
        end = System.currentTimeMillis()
        LOG.warn(s"Building Global Index Time: ${end - start} ms")
    }
  }

  buildIndex()


}