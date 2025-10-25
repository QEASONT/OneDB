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

package org.apache.spark.sql.execution.odb.partition.global

import org.apache.spark.{Partition, Partitioner}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

import scala.reflect.ClassTag

class ExactKeyPartitioner(expectedNumPartitions: Int) extends Partitioner {
  def numPartitions: Int = expectedNumPartitions

  def getPartition(key: Any): Int = {
    key match {
      case point: Point[Any] => point.metricIndex
      case _ => throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }
}

object ExactKeyPartitioner {

  def partition(dataRDD: RDD[Point[Any]], expectedNumPartitions: Int):
  (RDD[Point[Any]], ExactKeyPartitioner) = {
    val partitioner = new ExactKeyPartitioner(expectedNumPartitions)
    val pairedDataRDD = dataRDD.map(x => (x, None))
    val shuffled = new ShuffledRDD[Point[Any], Any, Any](pairedDataRDD, partitioner).map(_._1)
    (shuffled, partitioner)
  }
}
