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

package org.apache.spark.examples.sql.amds

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.amds.common.AMDSConfigConstants

// scalastyle:off println
object AMDSSQLExample {

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

    AMDSConfigConstants.DATA_TYPE = if (args.length > 1) args(1).toInt else 1
    AMDSConfigConstants.KNN = if (args.length > 2) args(2).toInt else 8
    AMDSConfigConstants.RANGE = if (args.length > 3) args(3).toDouble else 3800

    val SEARCH_TIME = if (args.length > 5) args(5).toInt else 3
    val metric = spark.sparkContext
      .textFile(textDir)
      .zipWithIndex()
      .filter(_._1 != "")


    if (AMDSConfigConstants.DATA_TYPE == 0) {
      val metricData = metric.map(getMetric)
      val df = metricData.toDF()
      df.createOrReplaceTempView("metric1")
      println(df.columns.toList)
      // create index for traj1
      var start = System.currentTimeMillis()
      spark.sql("CREATE AMDS INDEX metric1_index ON metric1 (metric)")
      var end = System.currentTimeMillis()
      println(s"Building Index time: ${end - start} ms")

      start = System.currentTimeMillis()
      val queryMetricStr = metricData.filter(t => t.id == 982).take(1)
        .map(point => s"AMDSPOINT(${point.metric.mkString(",")})").head
      println(queryMetricStr)

      val radius = AMDSConfigConstants.RANGE
      spark.sql(s"SELECT * FROM metric1 WHERE metric1.metric IN AMDSRANGE"
        + s"(" + queryMetricStr + s",$radius)").show()

      val k = AMDSConfigConstants.KNN
      spark.sql(s"SELECT * FROM metric1 WHERE EUCLID(metric1.metric, $queryMetricStr) KNN $k").show()
    } else {
      val metricData = metric.map(getMetricString)
      val df = metricData.toDF()
      df.createOrReplaceTempView("metric1")

      // create index for traj1
      var start = System.currentTimeMillis()
      spark.sql("CREATE AMDS INDEX metric1_index ON metric1 (metric)")
      var end = System.currentTimeMillis()
      println(s"Building Index time: ${end - start} ms")

      start = System.currentTimeMillis()
      val queryMetricStr = metricData.filter(t => t.id == 982).take(1)
        .map(point => s"AMDSPOINT('${point.metric}')").head
      println(queryMetricStr)

      val radius = AMDSConfigConstants.RANGE
      spark.sql(s"SELECT * FROM metric1 WHERE metric1.metric IN AMDSRANGE"
        + s"(" + queryMetricStr + s",$radius)").show()
      val k = AMDSConfigConstants.KNN
      spark.sql(s"SELECT * FROM metric1 WHERE EUCLID(metric1.metric, $queryMetricStr) KNN $k").show()
    }
    spark.stop()
  }
}
