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

package org.apache.spark.sql.execution.odb.index.local

import org.apache.spark.sql.catalyst.expressions.odb.common.metric.{MetricData, ODBSimilarity}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.odb.index.LocalIndex
import org.apache.spark.sql.execution.odb.partition.local.LocalODBPartitioner

case class LocalODBIndex(partitioner: LocalODBPartitioner) extends LocalIndex {


  def getResultWithThreshold(query: MetricData, threshold: Array[Double], nonZeroQueryM: Array[(Double, Int)]): Array[Long] = {
    val res = partitioner.getResultWithThreshold(query, threshold, nonZeroQueryM)
    res
  }

  def insertMetricData(query: MetricData): List[(MetricData, Double)] = {
    partitioner.insertMetricData(query)
  }

  def deleteMetricData(query: MetricData): List[(MetricData, Double)] = {
    partitioner.deleteMetricData(query)
  }

  //  def getPartition(key: Point[Any]): Int = {
  //    partitioner.getPartition(key)
  //  }

  def getResultWithKnn(query: MetricData, k: Int, nonZeroQueryM: Array[(Double, Int)]): Double = {
    partitioner.getResultWithKnn(query, k, nonZeroQueryM)
  }
}

object LocalODBIndex {
  def buildIndex(metricData: Array[MetricData]): LocalODBIndex = {
    val (_, partitioner) = LocalODBPartitioner.partition(metricData)

    LocalODBIndex(partitioner)

  }
}