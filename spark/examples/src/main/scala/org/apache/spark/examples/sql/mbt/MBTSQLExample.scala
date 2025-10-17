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

package org.apache.spark.examples.sql.mbt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.mbt.MBTSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.mbt.common.MBTConfigConstants

// scalastyle:off println
object MBTSQLExample {

  case class MetricRecord(id: Long, metric: Array[Double])

  case class MetricRecordString(id: Long, metric: String)

  private def getMetric(line: (String, Long)): MetricRecord = {
    val points = line._1.split(" ").map(x => x.toDouble)
    MetricRecord(line._2, points)
  }

  private def getMetricString(line: (String, Long)): MetricRecordString = {
    MetricRecordString(line._2, line._1)
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val textDir = if (args.length > 0) args(0) else "examples/src/main/resources/moby.txt"

    MBTConfigConstants.DATA_TYPE = if (args.length > 1) args(1).toInt else 0
    MBTConfigConstants.MBT_MODE = if (args.length > 2) args(2).toInt else 3
    MBTConfigConstants.KNN = if (args.length > 3) args(3).toInt else 8
    MBTConfigConstants.RANGE = if (args.length > 4) args(4).toDouble else 3800
    MBTConfigConstants.M_TREE_INNER_ORDER = if (args.length > 5) args(5).toInt else 8
    MBTConfigConstants.M_TREE_LEAF_ORDER = if (args.length > 6) args(6).toInt else 300
    MBTConfigConstants.LOCAL_M_TREE_INNER_ORDER = if (args.length > 7) args(7).toInt else 3
    MBTConfigConstants.LOCAL_M_TREE_LEAF_ORDER = if (args.length > 8) args(8).toInt else 30
    MBTConfigConstants.BPlusTreeOrder = if (args.length > 9) args(9).toInt else 30
    MBTConfigConstants.GlobalBPlusTreeOrder = if (args.length > 10) args(10).toInt else 3
    val distanceFunction = MBTConfigConstants.MBT_DISTANCE_TYPE match {
      case 0 => MBTSimilarityFunction.EUCLID
      case 1 => MBTSimilarityFunction.L1
      case 2 => MBTSimilarityFunction.COSINE
      case 3 => MBTSimilarityFunction.EDIT
    }

    val SEARCH_TIME = if (args.length > 5) args(5).toInt else 3
    val metric = spark.sparkContext
      .textFile(textDir)
      .zipWithIndex()
      .filter(_._1 != "")


    if (MBTConfigConstants.DATA_TYPE == 0) {
      val metricData = metric.map(getMetric)
      val df = metricData.toDF()
      df.createOrReplaceTempView("metric1")
      println(df.columns.toList)
      // create index for traj1
      var start = System.currentTimeMillis()
      println(s"CREATE MBT INDEX metric1_index ON metric1 ${distanceFunction.sql}(metric)")
      spark.sql(s"CREATE MBT INDEX metric1_index ON metric1 ${distanceFunction.sql}(metric)")
      var end = System.currentTimeMillis()
      println(s"Building Index time: ${end - start} ms")
      val queryMetricStr = metricData.filter(t => t.id == 982).take(1)
        .map(point => s"MBTPOINT(${point.metric.mkString(",")})").head
      println(queryMetricStr)

      val radius = MBTConfigConstants.RANGE
      spark.sql(s"SELECT * FROM metric1 WHERE metric1.metric IN MBTRANGE"
        + s"(" + queryMetricStr + s",$radius)").show()

      val k = MBTConfigConstants.KNN
      spark.sql(s"SELECT * FROM metric1 WHERE EUCLID(metric1.metric, $queryMetricStr) KNN $k").show()
    } else {
      val metricData = metric.map(getMetricString)
      val df = metricData.toDF()
      df.createOrReplaceTempView("metric1")

      // create index for traj1
      var start = System.currentTimeMillis()
      spark.sql("CREATE MBT INDEX metric1_index ON metric1 (metric)")
      var end = System.currentTimeMillis()
      println(s"Building Index time: ${end - start} ms")

      start = System.currentTimeMillis()
      val queryMetricStr = metricData.filter(t => t.id == 982).take(1)
        .map(point => s"MBTPOINT('${point.metric}')").head
      println(queryMetricStr)

      val radius = MBTConfigConstants.RANGE
      spark.sql(s"SELECT * FROM metric1 WHERE metric1.metric IN MBTRANGE"
        + s"(" + queryMetricStr + s",$radius)").show()
      val k = MBTConfigConstants.KNN
      spark.sql(s"SELECT * FROM metric1 WHERE EUCLID(metric1.metric, $queryMetricStr) KNN $k").show()
    }
    spark.stop()
  }
}
