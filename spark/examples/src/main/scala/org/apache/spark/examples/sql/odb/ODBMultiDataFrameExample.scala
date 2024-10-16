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
//      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

//    val imageDF = spark.read.format("org.apache.spark.ml.source.image.ImageFileFormat").load("examples/src/main/resources/1.png")
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val sc = spark.sparkContext
    // read from file
    val fileData = if (args.length > 0) args(0) else "examples/src/main/resources/dfood.txt"
    val queryData = if (args.length > 1) args(1) else "examples/src/main/resources/dfood_q.txt"
    ODBConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT = if (args.length > 2) args(2).toInt else 9
    ODBConfigConstants.SAMPLE_SIZE = if (args.length > 3) args(3).toLong else 1L * 10 * 1024 * 1024
    ODBConfigConstants.MIN_SAMPLE_SIZE = if (args.length > 4) args(4).toLong else 1L * 1024 * 1024
    ODBConfigConstants.MAX_SAMPLE_SIZE = if (args.length > 5) args(5).toLong else 2048L * 1024 * 1024
    ODBConfigConstants.RTREE_GLOBAL_MAX_ENTRIES_PER_NODE = if (args.length > 6) args(6).toInt else 5
    ODBConfigConstants.RTREE_LOCAL_MAX_ENTRIES_PER_NODE = if (args.length > 7) args(7).toInt else 5
    ODBConfigConstants.RTREE_GLOBAL_NUM_PARTITIONS = if (args.length > 8) args(8).toInt else 20
    ODBConfigConstants.RTREE_LOCAL_NUM_PARTITIONS = if (args.length > 9) args(9).toInt else 20


//    val fileData = "examples/src/main/resources/dfood.txt"
    val fileContents = sc.textFile(fileData)

    val headerInfo = fileContents.take(3)
    // read num of rows and columns
    val n = headerInfo(0).split(" ")(0).toInt
    val m = headerInfo(0).split(" ")(1).toInt

    // read metricm and metricMaxDis
    val metricM = headerInfo(1).split(" ").map(x => x.toInt)
    val metricMaxDis = headerInfo(2).split(" |\t").map(x => x.toDouble)
    val metricData = fileContents.zipWithIndex().filter(_._2 >= 3).map(MetricRecord.getMetric(_, metricM, metricMaxDis))
    //    val metricMeanDis = MetricRecord.calculateMetricMeanDis(metricData, metricM, metricMaxDis)

//    val queryData = "examples/src/main/resources/dfood_q.txt"
    val queryContents = sc.textFile(queryData)
    val queryHeaderInfo = queryContents.take(4)
    val queryM = queryHeaderInfo(0).split(" ").map(x => x.toInt)
    val queryKVal = queryHeaderInfo(1).split(" |\t").zipWithIndex.filter(_._2 > 0).map(x => x._1.toInt)
    val queryRad = queryHeaderInfo(3).split(" |\t").map(x => x.toDouble)
    val queryPos = queryContents.zipWithIndex().filter(_._2 >= 4).map(x => x._1.toInt).collect()


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
    //    val ff = spark.udf.register("udfDist", f)
    //    val aaa = ff(functions.lit(Array(1.0, 2.0)), functions.lit(Array(1.0, 2.0)))

    val df1 = metricData.toDF()
    df1.createOrReplaceTempView("metric1")
    //    df1.createODBIndex(df1("metricDouble"), df1("metricString"), df1("metricM"), df1("metricMaxDis"), "metric_index1")

    for (m <- queryPos.indices) {
      for (i <- queryRad.indices) {
        val radius = queryRad(i)
        val filterQueryP = metricData.filter(t => t.id == queryPos(m))
        if (filterQueryP.count() != 0) {
          val queryP = filterQueryP.take(1).head
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
      }
      for (i <- queryKVal.indices) {
        val k = queryKVal(i)
        val filterQueryP = metricData.filter(t => t.id == queryPos(m))
        if (filterQueryP.count() != 0) {
          val queryP = filterQueryP.take(1).head
          val pintsArray: Array[Point[Any]] = metricM.zipWithIndex.map(x =>
            x._1 match {
              case 0 | 1 | 2 | 3 | 4 | 5 =>
                Point[Any](queryP.metricDouble(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
              case 6 =>
                Point[Any](queryP.metricString(x._2), x._2, x._1, metricMaxDis(x._2), queryP.id)
            })
          val searchMetric = MetricData(pintsArray)
          //          df1.odbSimilarityWithKNNSearch(searchMetric, k, queryM, df1("metricDouble")).show()
          df1.odbSimilarityWithKNNSearch(searchMetric, k, queryM, df1("metricDouble"))
            .map(
              attr => {
                val id = attr.getAs[Long](0)
                val metricDouble = attr.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]](1).map(
                  x => if (x == null) null else x.toArray).toArray
                val metricString = attr.getAs[mutable.WrappedArray[String]](2).map(
                  x => if (x == null) null else x).toArray
                val metricRecord = MetricRecord(id, metricDouble, metricString, metricM, metricMaxDis)
                (id, metricRecord.toString)
              }
            ).toDF("ID", "MetricRecord").show(20, false)
        }
      }
    }

    spark.stop()
  }
}
