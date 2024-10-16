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

package org.apache.spark.sql.execution.odb.algorithms

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.odb.PackedPartition
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.{Point, Shape}
import org.apache.spark.sql.execution.odb.index.global.{GlobalBPlusTreeIndex, GlobalODBIndex}
import org.apache.spark.sql.execution.odb.index.local.{LocalMTreeIndex, LocalODBIndex, LocalPivotIndex, LocalSimpleIndex}
import org.apache.spark.sql.execution.odb.rdd.ODBRDD

import scala.tools.nsc.interpreter.Completion.Candidates

object MetricRangeAlgorithms {
  // search from M-tree
  def localSearch(query: MetricData, packedPartition: PackedPartition,
                  thresholdArray: Array[Double], nonZeroMetric: Array[(Int, Int)]):
  Iterator[Long] = {

    val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalODBIndex]).head
      .asInstanceOf[LocalODBIndex]
    val answers = localIndex.getResultWithThreshold(query, thresholdArray, nonZeroMetric).iterator
    answers
  }

  def localValid(metricData: MetricData, candidates: RDD[MetricData], threshold: Double, nonZeroMetric: Array[(Int, Int)]):
  RDD[(MetricData, Double)] = {
    val sum = nonZeroMetric.map(_._1).sum
    val weight = nonZeroMetric.map(x => (x._1 / sum, x._2))

    val res = candidates.map(candidateMD => {
      // (value index) index new
      val dist = weight.map(
        queryM => {
          //          val queryPoint = metricData.points(queryM._1._2)
          //          val candidatePoint = candidateMD.points(queryM._1._2)
          val distance = metricData.points(queryM._2).minDist(candidateMD.points(queryM._2)) * queryM._1
          distance
        }
      ).sum
      (candidateMD, dist)
    })
    //    val cc = res.collect()
    res.filter(_._2 <= threshold)
  }

  //  }

  object DistributedSearch extends Logging {

    def search(sparkContext: SparkContext, query: MetricData, odbRDD: ODBRDD,
               threshold: Double, queryM: Array[Int]): RDD[(MetricData, Double)] = {
      val bQuery = sparkContext.broadcast(query)
      val averageThreshold = threshold / queryM.sum
      val thresholdArray = queryM.map(x => x * averageThreshold)
      val globalODBIndex = odbRDD.globalODBIndex.asInstanceOf[GlobalODBIndex]

      var start = System.currentTimeMillis()
      var end = start
      val nonZeroMetric = queryM.zipWithIndex.filter(x => x._1 != 0)
      val candidatePartitions = globalODBIndex.getPartitionsWithThreshold(query, threshold, nonZeroMetric)
      logWarning(s"ODB Get candidatePartitions: ${
        end - start
      } ms")

      val candidateID = PartitionPruningRDD.create(odbRDD.packedRDD, candidatePartitions.contains).flatMap(packedPartition =>
        localSearch(bQuery.value, packedPartition, thresholdArray, nonZeroMetric)
      ).distinct()
//            val cc = candidateID.collect()
      //      val candidateMetric = odbRDD.metricDataRDD.filter(x => candidateID.collect().contains(x.id))
      val rdd1 = odbRDD.metricDataRDD.map(x => (x.id, x))
      val rdd2 = candidateID.map(x => (x, null))
      val candidateMetric = rdd1.join(rdd2).map(_._2._1)

      end = System.currentTimeMillis()
      logWarning(s"ODB Get candidateArray: ${
        end - start
      } ms")

      start = System.currentTimeMillis()
      //      val dd = localValid(bQuery.value, odbRDD.metricDataRDD, threshold, nonZeroMetric).collect()
      val answers = localValid(bQuery.value, candidateMetric, threshold, nonZeroMetric)
      end = System.currentTimeMillis()
      logWarning(s"ODB Get Result of LocalSearch: ${
        end - start
      } ms")
      //      val cc = answers.collect()
      answers

    }
  }

}

