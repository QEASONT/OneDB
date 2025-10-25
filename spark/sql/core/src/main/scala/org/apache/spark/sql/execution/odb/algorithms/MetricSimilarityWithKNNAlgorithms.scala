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

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.odb.PackedPartition
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.{MetricData, ODBSimilarity}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point
import org.apache.spark.sql.execution.odb.index.global.{GlobalBPlusTreeIndex, GlobalODBIndex}
import org.apache.spark.sql.execution.odb.index.local.LocalODBIndex
import org.apache.spark.sql.execution.odb.index.local.LocalMTreeIndex
import org.apache.spark.sql.execution.odb.index.local.LocalSimpleIndex
import org.apache.spark.sql.execution.odb.index.local.LocalPivotIndex
import org.apache.spark.sql.execution.odb.rdd.ODBRDD

import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

object MetricSimilarityWithKNNAlgorithms {

  object DistributedSearch extends Logging {
    implicit val order = new Ordering[(MetricData, Double)] {
      def compare(x: (MetricData, Double), y: (MetricData, Double)): Int = {
        x._2.compare(y._2)
      }
    }

    def localValid(metricData: MetricData, candidates: RDD[MetricData], nonZeroMetric: Array[(Double, Int)], k: Int): RDD[(MetricData, Double)] = {
      val sum = nonZeroMetric.map(_._1).sum
      val weight = nonZeroMetric.map(x => (x._1 / sum.toDouble, x._2))

      val res = candidates.map(candidateMD => {
        val dist = weight.map(
          queryM => {
            val distance = metricData.points(queryM._2).minDist(candidateMD.points(queryM._2)) * queryM._1
            distance
          }
        ).sum
        (candidateMD, dist)
      })


      // Take the top k from the filtered results and convert to an RDD
      val topKResults = res.takeOrdered(k)(Ordering.by[(MetricData, Double), Double](_._2))

      // Convert the array back to an RDD
      res.sparkContext.parallelize(topKResults)
    }

    def localSearch(query: MetricData, packedPartition: PackedPartition,
                    threshold: Array[Double], nonZeroMetric: Array[(Double, Int)]):
    Iterator[Long] = {

      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalODBIndex]).head
        .asInstanceOf[LocalODBIndex]
      val answers = localIndex.getResultWithThreshold(query, threshold, nonZeroMetric).iterator
      answers
    }

    def localKnnSearch(query: MetricData, packedPartition: PackedPartition,
                       count: Int, nonZeroMetric: Array[(Double, Int)]):
    Iterator[Double] = {

      val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalODBIndex]).head
        .asInstanceOf[LocalODBIndex]
      val answers = List(localIndex.getResultWithKnn(query, count, nonZeroMetric)).toIterator
      answers
    }


    def search(sparkContext: SparkContext, query: MetricData, odbRDD: ODBRDD,
               count: Int, queryM: Array[Int]): RDD[(MetricData, Double)] = {

      val bQuery = sparkContext.broadcast(query)
      val globalODBIndex = odbRDD.globalODBIndex.asInstanceOf[GlobalODBIndex]

      var start = System.currentTimeMillis()
      var end = start

      val nonZeroMetric = queryM.zipWithIndex.filter(x => x._1 != 0)
      val sum = nonZeroMetric.map(_._1).sum
      val weight = nonZeroMetric.map(x => (x._1 / sum.toDouble, x._2))
      val estimatedRated = sparkContext.getConf.getDouble("spark.odb.estimatedRate", ODBConfigConstants.ODB_KNN_ESTIMATED_RATE_DEFAULT)
      val estimatedDis = globalODBIndex.getKnnEstimatedThreshold(bQuery.value, count, weight) * estimatedRated
      end = System.currentTimeMillis()
      logWarning(s"ODB Get Estimated Distance of kNN: ${
        end - start
      } ms")


      start = System.currentTimeMillis()
      val byIdx: Map[Int, Double] = weight.map { case (w, idx) => idx -> w }.toMap
      val thresholdArray: Array[Double] = queryM.indices.map(i => estimatedDis * byIdx.getOrElse(i, 0.0d)).toArray

      val candidatePartitions = globalODBIndex.getPartitionsWithThreshold(bQuery.value, estimatedDis, weight)

      end = System.currentTimeMillis()
      logWarning(s"ODB Get candidatePartitions: ${
        end - start
      } ms")



      val candidateID = PartitionPruningRDD.create(odbRDD.packedRDD, candidatePartitions.contains)
        .mapPartitions { iter =>
          iter.flatMap(packedPartition => {
            localSearch(bQuery.value, packedPartition, thresholdArray, weight)
          })
        }

      val bCandidateID = sparkContext.broadcast(candidateID.collect().toSet)
      val candidateMetric = odbRDD.metricDataRDD.filter(x => bCandidateID.value.contains(x.id))
      candidateMetric.foreach(_ => ())

      start = System.currentTimeMillis()

      val answers = localValid(bQuery.value, candidateMetric, weight, count)
      answers.foreach(_ => ())
      end = System.currentTimeMillis()
      logWarning(s"ODB Get Result of Knn: ${
        end - start
      } ms")

      answers
    }
  }
}
