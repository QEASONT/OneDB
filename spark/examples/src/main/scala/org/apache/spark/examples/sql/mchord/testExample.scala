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
package org.apache.spark.examples.sql.mchord

import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.execution.dita.partition.global.GlobalSTRPartitioner
import org.apache.spark.sql.execution.dita.partition.{STRPartitioner, TriePartitioner}

import org.apache.spark.sql.SparkSession

object testExample {
  case class MetricRecord(id: Long, metric: Array[Double])

  private def getMetric(line: (String, Long)): Point = {
    val points = line._1.split(" ").map(x => x.toDouble)
    Point(points)
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val metricData = spark.sparkContext
      .textFile("examples/src/main/resources/LA.txt")
      .zipWithIndex()
      .filter(_._2 > 1)
      .map(getMetric)
    val (partitionedRDD, partitioner) = GlobalSTRPartitioner.partitionRDD(metricData,
      2, 5)

  }
}