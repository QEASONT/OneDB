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

package org.apache.spark.examples.sql.odb

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.odb.ODBSimilarityFunction
import org.apache.spark.sql.catalyst.expressions.odb.common.ODBConfigConstants
import org.apache.spark.sql.execution.odb.util.MetricRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.odb.common.metric.MetricData
import org.apache.spark.sql.catalyst.expressions.odb.common.shape.Point

import scala.collection.mutable

object ODBMultiDataFrameExample extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      //      .master("spark://node20:7077")
//                  .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //    val imageDF = spark.read.format("org.apache.spark.ml.source.image.ImageFileFormat").load("examples/src/main/resources/1.png")
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val sc = spark.sparkContext
    // read from file
    val fileData = if (args.length > 0) args(0) else "examples/src/main/resources/dair.txt"
    val queryData = if (args.length > 1) args(1) else "examples/src/main/resources/dair_q.txt"

    val searchTime = sc.getConf.getInt("spark.sql.odb.searchTime", 5)

    val INSERT_OR_NOT = if (args.length > 4) args(5).toInt else 0
    val INSERT_NUM = if (args.length > 5) args(5).toInt else 100
    //    val fileData = "examples/src/main/resources/dfood.txt"
    val numPartitionOfInputData = sc.getConf.getInt("spark.sql.odb.shuffle.partitions", 500)
    val fileContents = sc.textFile(fileData, numPartitionOfInputData)

    val headerInfo = fileContents.take(3)
    // read num of rows and columns
    val n = headerInfo(0).split(" ")(0).toInt
    val m = headerInfo(0).split(" ")(1).toInt

    // read metricm and metricMaxDis
    val metricM = headerInfo(1).split(" ").map(x => x.toInt)
    val metricMaxDis = headerInfo(2).split(" |\t").map(x => x.toDouble)
    val metricData = fileContents.mapPartitionsWithIndex { (idx, iter) =>
        if (idx == 0) iter.drop(3) else iter
      }.zipWithUniqueId()
      .map(MetricRecord.getMetric(_, metricM, metricMaxDis))
    //    val metricMeanDis = MetricRecord.calculateMetricMeanDis(metricData, metricM, metricMaxDis)

    //    val queryData = "examples/src/main/resources/dfood_q.txt"
    val queryContents = sc.textFile(queryData)
    val queryHeaderInfo = queryContents.take(4)
    val queryM = queryHeaderInfo(0).split(" ").map(x => x.toInt)
    val queryKVal = queryHeaderInfo(1).split(" |\t").zipWithIndex.filter(_._2 > 0).map(x => x._1.toInt)
    val queryRad = queryHeaderInfo(3).split(" |\t").map(x => x.toDouble)
    //    val queryPos = queryContents.zipWithIndex().filter(_._2 >= 4).map(x => x._1.toInt).collect()


    val f = (t1: Any, t2: Any) => {
      def calDist(t1: Array[Double], t2: Array[Double]): Double = {
        require(t1.length == t2.length)
        var ans = 0.0
        for (i <- t1.indices)
          ans += (t1(i) - t2(i)) * (t1(i) - t2(i))
        Math.sqrt(ans)
      }

      t1 match {
        case doubles: Array[Double] =>
          t2 match {
            case doubles2: Array[Double] =>
              calDist(doubles, doubles2)
            case point: Point[Any] =>
              calDist(doubles, point.coord.asInstanceOf[Array[Double]])
            case _ =>
              0
          }
        case _ => 0
      }
    }

    val df1 = metricData.toDF()
    df1.createOrReplaceTempView("metric1")
    df1.createODBIndex(df1("metricDouble"), df1("metricString"), df1("metricM"), df1("metricMaxDis"), "metric_index1")


    for (m <- 1 to searchTime) {
      for (i <- queryRad.indices) {
        val queryP = metricData.takeSample(false, 1).head
        logWarning(s"Range Search for Query Point ID: ${queryP.id} with Radius: ${queryRad(i)}")
        val radius = queryRad(i)
        val pintsArray: Array[Point[Any]] = metricM.zipWithIndex.map(x =>
          x._1 match {
            case 0 | 1 | 2 | 3 | 4 | 5 =>
              Point[Any](queryP.metricDouble(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
            case 6 =>
              Point[Any](queryP.metricString(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
          })
        val searchMetric = MetricData(pintsArray)
        df1.odbSimilarityWithRangeSearch(searchMetric, radius, queryM, df1("metricDouble")).show()

      }
      for (i <- queryKVal.indices) {

        val queryP = metricData.takeSample(false, 1).head
        logWarning(s"kNN Search for Query Point ID: ${queryP.id} with k: ${queryKVal(i)}")
        val k = queryKVal(i)

        val pintsArray: Array[Point[Any]] = metricM.zipWithIndex.map(x =>
          x._1 match {
            case 0 | 1 | 2 | 3 | 4 | 5 =>
              Point[Any](queryP.metricDouble(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
            case 6 =>
              Point[Any](queryP.metricString(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
          })
        val searchMetric = MetricData(pintsArray)
        df1.odbSimilarityWithKNNSearch(searchMetric, k, queryM, df1("metricDouble")).show()

      }

    }


    spark.stop()
  }
}
