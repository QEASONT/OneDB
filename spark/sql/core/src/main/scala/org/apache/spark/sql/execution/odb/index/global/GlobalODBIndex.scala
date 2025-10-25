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

package org.apache.spark.sql.execution.odb.index.global

import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Shape
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.{MetricData, ODBSimilarity}
import org.apache.spark.sql.catalyst.expressions.odb.index.GlobalIndex
import org.apache.spark.sql.execution.odb.partition.global.GlobalODBPartitioner

import scala.collection.mutable.ArrayBuffer

case class GlobalODBIndex(partitioner: GlobalODBPartitioner) extends GlobalIndex {
  def getPartition(key: MetricData): Int = {
    partitioner.getPartition(key)
  }

  def getKnnSamplePartitions(query: MetricData, nonZeroQueryM: Array[(Double, Int)]): List[Int] = {
    partitioner.getKnnSamplePartitions(query, nonZeroQueryM)
  }

  def getPartitionsWithThreshold(key: MetricData, threshold: Double,
                                 nonZeroQueryM: Array[(Double, Int)]): List[Int] = {
    partitioner.getPartitionsWithThreshold(key, threshold, nonZeroQueryM)
  }

  def getKnnEstimatedThreshold(query: MetricData, k: Int, nonZeroQueryM: Array[(Double, Int)]): Double = {
    partitioner.getKnnEstimatedThreshold(query, k, nonZeroQueryM)
  }

  //
  //  def getPartitionsWithDistances(key: Trajectory, distanceFunction: TrajectorySimilarity, threshold: Double): List[(Int, Double)] = {
  //    partitioner.getPartitions(key, distanceFunction, threshold, 0.0)
  //  }
}